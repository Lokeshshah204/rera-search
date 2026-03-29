'use strict';

/**
 * emailDocuments.js
 * Background email delivery for RERA project documents.
 *
 * Flow:
 *   1. enqueueJob(projectId, email, projectName) → { jobId, position }
 *   2. Background worker picks up jobs sequentially (one at a time).
 *   3. Worker fetches all docs as arraybuffers (p-limit 3, 90s timeout each).
 *   4. Builds ZIP in memory via archiver.
 *   5. Sends ZIP as email attachment via nodemailer.
 *
 * Job statuses: queued → building → sending → done | failed
 */

const crypto    = require('crypto');
const https     = require('https');
const constants = require('constants');
const archiver  = require('archiver');
const axios     = require('axios');
const nodemailer= require('nodemailer');
const pLimit    = require('p-limit');
const { PassThrough } = require('stream');

const fetchProjectDocuments = require('./fetchProjectDocuments');

// ── SMTP config ──────────────────────────────────────────────────────────────
function isSmtpConfigured() {
  return !!(
    process.env.SMTP_HOST &&
    process.env.SMTP_PORT &&
    process.env.SMTP_USER &&
    process.env.SMTP_PASS &&
    process.env.SMTP_FROM
  );
}

function createTransporter() {
  const port   = parseInt(process.env.SMTP_PORT || '587', 10);
  return nodemailer.createTransport({
    host  : process.env.SMTP_HOST,
    port,
    secure: port === 465,
    auth  : { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  });
}

// ── In-memory job store ───────────────────────────────────────────────────────
const jobQueue = [];       // pending jobs (FIFO)
const jobMap   = new Map();// jobId → job object (for status lookups)
let   activeJob = null;    // currently-processing job

const MAX_QUEUE_SIZE = 20; // reject new jobs when queue is this full
const JOB_MAP_CAP   = 500; // evict old done/failed entries when map exceeds this

// ── VDMS fetch agent (same SSL workaround as proxy-server.js) ────────────────
const VDMS_AGENT = new https.Agent({
  rejectUnauthorized: false,
  secureOptions: constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

const DOC_TIMEOUT_MS = 90_000;  // 90s per document — no browser waiting, can be generous
const DOC_CONCURRENCY = 3;      // lower than the interactive route; background = lower priority
const DOC_MAX_BYTES   = 20 * 1024 * 1024; // 20 MB per-doc cap

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Queue a new email-delivery job.
 * Idempotent: returns existing job if the same email+project is already queued/active.
 * @returns {{ jobId:string, position:number, existing:boolean }}
 */
function enqueueJob(projectId, email, projectName) {
  // Idempotency check
  for (const [id, job] of jobMap) {
    if (
      job.projectId === projectId &&
      job.email     === email &&
      ['queued', 'building', 'sending'].includes(job.status)
    ) {
      const position = jobQueue.findIndex(j => j.jobId === id) + 1;
      return { jobId: id, position: position || 0, existing: true };
    }
  }

  // Queue capacity guard
  if (jobQueue.length >= MAX_QUEUE_SIZE) {
    const err = new Error('Server is busy. Please try again in a few minutes.');
    err.code = 'QUEUE_FULL';
    throw err;
  }

  const jobId = crypto.randomUUID();
  const job = {
    jobId,
    projectId,
    email,
    projectName : projectName || `Project #${projectId}`,
    status      : 'queued',
    progress    : { fetched: 0, total: 0 },
    error       : null,
    createdAt   : new Date().toISOString(),
    finishedAt  : null,
  };

  jobQueue.push(job);
  jobMap.set(jobId, job);
  evictOldJobs();

  // Kick the worker (no-op if already running)
  setImmediate(processNextJob);

  return { jobId, position: jobQueue.length, existing: false };
}

/**
 * Get job status by ID.
 * Returns a public-safe object (email is omitted).
 */
function getJobStatus(jobId) {
  const job = jobMap.get(jobId);
  if (!job) return null;
  return {
    jobId      : job.jobId,
    projectId  : job.projectId,
    projectName: job.projectName,
    status     : job.status,
    progress   : job.progress,
    error      : job.error,
    createdAt  : job.createdAt,
    finishedAt : job.finishedAt,
  };
}

// ── Background worker ─────────────────────────────────────────────────────────

async function processNextJob() {
  if (activeJob !== null || jobQueue.length === 0) return;

  activeJob = jobQueue.shift();
  const job = activeJob;

  console.log(`[EmailDocs] Starting job ${job.jobId} — project ${job.projectId} → ${job.email}`);

  try {
    // 1. Get document list
    job.status = 'building';
    const docs = await fetchProjectDocuments(job.projectId);
    job.progress.total = docs.length;

    if (docs.length === 0) {
      throw new Error('No documents found for this project in the RERA database.');
    }

    // 2. Build ZIP in memory
    const zipBuffer = await buildZip(docs, job);

    // 3. Send email
    job.status = 'sending';
    console.log(`[EmailDocs] Job ${job.jobId} — ZIP is ${(zipBuffer.length / 1024 / 1024).toFixed(1)} MB, sending email…`);
    await sendEmail(job, docs, zipBuffer);

    job.status     = 'done';
    job.finishedAt = new Date().toISOString();
    console.log(`[EmailDocs] Job ${job.jobId} — done.`);

  } catch (err) {
    job.status     = 'failed';
    job.error      = err.message;
    job.finishedAt = new Date().toISOString();
    console.error(`[EmailDocs] Job ${job.jobId} failed:`, err.message);
  } finally {
    activeJob = null;
    // Schedule next job without growing the call stack
    setImmediate(processNextJob);
  }
}

// ── ZIP builder ───────────────────────────────────────────────────────────────

function buildZip(docs, job) {
  return new Promise((resolve, reject) => {
    const arc     = archiver('zip', { zlib: { level: 5 } });
    const pass    = new PassThrough();
    const chunks  = [];

    pass.on('data',  chunk => chunks.push(chunk));
    pass.on('end',   ()    => resolve(Buffer.concat(chunks)));
    pass.on('error', reject);
    arc.on('error',  reject);

    arc.pipe(pass);

    const limit = pLimit(DOC_CONCURRENCY);

    const tasks = docs.map(doc => limit(async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), DOC_TIMEOUT_MS);
      try {
        const resp = await axios.get(doc.url, {
          httpsAgent      : VDMS_AGENT,
          responseType    : 'arraybuffer',
          signal          : controller.signal,
          maxContentLength: DOC_MAX_BYTES,
          headers         : { 'User-Agent': 'Mozilla/5.0 (compatible; GujreraExperts/1.0)' },
        });
        clearTimeout(timer);
        arc.append(Buffer.from(resp.data), { name: doc.filename });
        job.progress.fetched++;
        console.log(`[EmailDocs] Job ${job.jobId} — fetched ${job.progress.fetched}/${job.progress.total}: ${doc.filename}`);
      } catch (err) {
        clearTimeout(timer);
        console.warn(`[EmailDocs] Skipped ${doc.filename}:`, err.message);
        const placeholder = `Document not available: ${doc.label}\nURL: ${doc.url}\nReason: ${err.message}`;
        arc.append(placeholder, { name: doc.filename.replace(/\.pdf$/, '_UNAVAILABLE.txt') });
      }
    }));

    Promise.all(tasks)
      .then(() => arc.finalize())
      .catch(reject);
  });
}

// ── Email sender ──────────────────────────────────────────────────────────────

async function sendEmail(job, docs, zipBuffer) {
  const transporter = createTransporter();
  const fetched     = job.progress.fetched;
  const total       = job.progress.total;
  const unavailable = total - fetched;

  const subject = `RERA Documents — ${job.projectName} (Project ID: ${job.projectId})`;

  const textBody = [
    `Hello,`,
    ``,
    `Your requested RERA documents for the following project are attached:`,
    ``,
    `  Project: ${job.projectName}`,
    `  Project ID: ${job.projectId}`,
    `  Documents fetched: ${fetched} of ${total}`,
    unavailable > 0 ? `  Unavailable: ${unavailable} (included as _UNAVAILABLE.txt placeholders in the ZIP)` : '',
    ``,
    `The ZIP file contains all documents uploaded by the promoter to the Gujarat RERA public database.`,
    ``,
    `Important: This information is sourced from the official Gujarat RERA public database.`,
    `GujreraExperts.com is an independent platform and is not affiliated with Gujarat RERA.`,
    `This is not legal advice. Verify all critical documents on the official RERA portal.`,
    `  https://gujrera.gujarat.gov.in/#/project-details/${job.projectId}`,
    ``,
    `— GujreraExperts.com`,
  ].filter(l => l !== undefined).join('\n');

  const htmlBody = `
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1a1a2e">
  <div style="background:#1a1a2e;padding:24px 32px;border-radius:8px 8px 0 0">
    <h2 style="color:#fff;margin:0;font-size:18px">RERA Documents Ready</h2>
    <p style="color:#a0aec0;margin:6px 0 0">GujreraExperts.com</p>
  </div>
  <div style="background:#f8fafc;padding:32px;border-radius:0 0 8px 8px;border:1px solid #e2e8f0">
    <p style="margin:0 0 16px">Your requested RERA documents are attached as a ZIP file.</p>
    <table style="width:100%;border-collapse:collapse;background:#fff;border-radius:6px;overflow:hidden;border:1px solid #e2e8f0">
      <tr style="background:#f1f5f9"><td style="padding:10px 16px;font-weight:600;font-size:13px;color:#475569">Project</td>
        <td style="padding:10px 16px;font-size:13px">${escHtml(job.projectName)}</td></tr>
      <tr><td style="padding:10px 16px;font-weight:600;font-size:13px;color:#475569;border-top:1px solid #f1f5f9">Project ID</td>
        <td style="padding:10px 16px;font-size:13px;border-top:1px solid #f1f5f9">${job.projectId}</td></tr>
      <tr style="background:#f1f5f9"><td style="padding:10px 16px;font-weight:600;font-size:13px;color:#475569">Documents</td>
        <td style="padding:10px 16px;font-size:13px">${fetched} of ${total} fetched${unavailable > 0 ? ` (${unavailable} unavailable — see .txt placeholders in ZIP)` : ''}</td></tr>
    </table>
    ${unavailable > 0 ? `<p style="margin:16px 0 0;font-size:12px;color:#64748b">Some documents could not be downloaded from RERA's servers at the time of processing. Their filenames appear in the ZIP as <em>_UNAVAILABLE.txt</em> placeholders.</p>` : ''}
    <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0">
    <p style="font-size:11px;color:#94a3b8;margin:0">All information is sourced from the official Gujarat RERA public database.
    GujreraExperts.com is an independent platform and is not affiliated with Gujarat RERA.
    This email is not legal advice. Verify documents on the
    <a href="https://gujrera.gujarat.gov.in/#/project-details/${job.projectId}" style="color:#1a1a2e">official RERA portal</a>.</p>
  </div>
</div>`;

  const zipSizeMB = (zipBuffer.length / 1024 / 1024).toFixed(1);
  if (zipBuffer.length > 24 * 1024 * 1024) {
    console.warn(`[EmailDocs] ZIP is ${zipSizeMB} MB — may exceed Gmail's 25 MB attachment limit. Sending anyway.`);
  }

  await transporter.sendMail({
    from       : process.env.SMTP_FROM,
    to         : job.email,
    subject,
    text       : textBody,
    html       : htmlBody,
    attachments: [{
      filename   : `RERA_${job.projectId}_Documents.zip`,
      content    : zipBuffer,
      contentType: 'application/zip',
    }],
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function escHtml(str) {
  return String(str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function evictOldJobs() {
  if (jobMap.size <= JOB_MAP_CAP) return;
  // Remove oldest done/failed entries first
  for (const [id, job] of jobMap) {
    if (['done', 'failed'].includes(job.status)) {
      jobMap.delete(id);
      if (jobMap.size <= JOB_MAP_CAP) break;
    }
  }
}

module.exports = { enqueueJob, getJobStatus, isSmtpConfigured };
