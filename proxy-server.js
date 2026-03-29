/**
 * Gujarat RERA Proxy Server — gujreraexperts.com
 * ──────────────────────────────────────────────
 * Full buyer report: complaints, units, compliance, bank, amenities,
 * land, other projects, promoter details, risk score.
 *
 * Run: node proxy-server.js
 */

'use strict';

require('dotenv').config();   // loads OPENROUTER_API_KEY from .env if present

const express  = require('express');
const axios    = require('axios');
const cors     = require('cors');
const https    = require('https');
const fs       = require('fs');
const path     = require('path');
const nodeCron = require('node-cron');
const { getProjectInsights } = require('./utils/decisionEngine');
const { generateHash }       = require('./change-detector');

const app  = express();
const PORT = process.env.PORT || 3000;

/* ── Change-detection store: projectId → last-seen hash ── */
const projectHashStore = new Map();

/* ── Per-IP rate limiter for document download (1 req / 10s) ── */
const downloadRateLimiter = new Map(); // ip → lastDownloadTimestamp
function checkDownloadRateLimit(ip) {
  const now  = Date.now();
  const last = downloadRateLimiter.get(ip) || 0;
  if (now - last < 10_000) return false; // blocked
  downloadRateLimiter.set(ip, now);
  // Periodically purge old entries to prevent memory leak
  if (downloadRateLimiter.size > 5000) {
    for (const [k, v] of downloadRateLimiter) {
      if (now - v > 60_000) downloadRateLimiter.delete(k);
    }
  }
  return true;
}

/* ── SSL agent — Gujarat RERA uses legacy TLS renegotiation ── */
const SSL_AGENT = new https.Agent({
  secureOptions: require('constants').SSL_OP_LEGACY_SERVER_CONNECT,
  rejectUnauthorized: false,
});

/* ── Local disk cache ── */
const CACHE_DIR    = path.join(__dirname, 'cache');
const CACHE_TTL_MS =  1 * 60 * 60 * 1000;   // 1 hour — RERA updates during 10:00–19:00 IST
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

function cacheRead(key) {
  try {
    const p    = path.join(CACHE_DIR, `${key}.json`);
    const stat = fs.statSync(p);
    if (Date.now() - stat.mtimeMs > CACHE_TTL_MS) return null;   // stale
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch { return null; }
}

function cacheWrite(key, data) {
  try {
    fs.writeFileSync(path.join(CACHE_DIR, `${key}.json`), JSON.stringify(data));
  } catch (e) { console.warn('[Cache] Write failed:', e.message); }
}

/* ── Rate-limit helper ── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
const INDEX_BATCH_SIZE = 1000;   // IDs per maplocation request
const RATE_DELAY_MS    = 150;    // ms pause between index batches

/* ── CORS ── */
app.use(cors({
  origin: [
    'https://gujreraexperts.com',
    'https://www.gujreraexperts.com',
    'http://localhost',
    'http://127.0.0.1',
    /^http:\/\/localhost:\d+$/,
    /^file:\/\//,
  ],
}));
app.use(express.json());

/* ── Serve frontend ── */
// index.html: always re-validate (no browser cache) so UI changes take effect immediately
app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(require('path').join(__dirname, 'index.html'));
});
app.use(express.static(__dirname));

/* ── API base URLs ── */
const RERA_BASE          = 'https://gujrera.gujarat.gov.in';
const MAP_API            = `${RERA_BASE}/maplocation/public/getProjectMapView`;
const PROJECT_API        = `${RERA_BASE}/project_reg/public/alldatabyprojectid`;
const FORM1_API          = `${RERA_BASE}/formone/public/getfrom-one-progress-report-by-project-id`;
const FORM1_FULL_API     = `${RERA_BASE}/formone/public/getfrom-one-byformone-id`;
const FORM2_API          = `${RERA_BASE}/formtwo/public/getform-two-by-id`;
const FORM3_API          = `${RERA_BASE}/formthree/public`;
const QUARTER_API        = `${RERA_BASE}/quarter/public`;
const FORM5_API          = `${RERA_BASE}/form_five`;
const DASHBOARD_API      = `${RERA_BASE}/dashboard`;
const BANKS_API              = `${RERA_BASE}/project_reg/public/project-app/getproject-banks`;
const BANKS_WITHDRAWAL_API   = `${RERA_BASE}/project_reg/public/project-app/getproject-banks-withdrawal`;
const QE_RETURN_API          = `${RERA_BASE}/quarter/get-by-project-id`;  // Q-E return status + document UIDs (public, no auth)
const LAND_API           = `${RERA_BASE}/project_reg/public/land-owner/project`;
const PREV_PROJECTS_API  = `${RERA_BASE}/project_reg/public/getprev-project-list`;
const PROMOTER_DET_API   = `${RERA_BASE}/project_reg/public/getproject-details`;
const PROJECT_APPS_API   = `${RERA_BASE}/project_reg/public/projectAllApplications`;  // source 1: scoped apps
const COMPLAINT_API      = `${RERA_BASE}/complain/public/filter-hearing-details`;     // source 2: complaint hearings
const PROMOTER_GROUP_API = `${RERA_BASE}/user_reg/promoter/promoter`;
const POSSESSION_API     = `${RERA_BASE}/formone/public/project/noc/getnoc-by-proj-proid-type`;

const HEADERS = {
  'User-Agent'     : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept'         : 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Referer'        : `${RERA_BASE}/`,
  'Origin'         : RERA_BASE,
};

/* ─────────────────────────────────────────────────────────────────
   In-memory search index
   ───────────────────────────────────────────────────────────────── */
let searchIndex    = [];
let indexBuiltAt   = null;
let indexBuilding  = false;
const INDEX_MAX_ID = 35000;
const INDEX_TTL_MS = 1 * 60 * 60 * 1000;   // 1 hour — keeps search index in sync with project cache

async function buildIndex() {
  if (indexBuilding) return;

  /* ── Serve from disk cache if fresh (avoids re-fetching on every restart) ── */
  const cached = cacheRead('index');
  if (cached && Array.isArray(cached) && cached.length > 0) {
    searchIndex  = cached;
    indexBuiltAt = Date.now();
    console.log(`[Index] Loaded ${searchIndex.length} projects from cache (skipping API).`);
    return;
  }

  indexBuilding = true;
  console.log(`[Index] Building search index in batches of ${INDEX_BATCH_SIZE} (150 ms delay between batches)…`);
  try {
    const ids         = Array.from({ length: INDEX_MAX_ID }, (_, i) => i + 1);
    const allProjects = [];

    for (let start = 0; start < ids.length; start += INDEX_BATCH_SIZE) {
      const batch = ids.slice(start, start + INDEX_BATCH_SIZE);
      const res   = await axiosPost(MAP_API, batch, 60000).catch(() => null);
      if (res?.data) {
        console.log(`[Index] Batch response sample:`, JSON.stringify(res.data[0]).slice(0,100));
        allProjects.push(...res.data);
      } else {
        console.log(`[Index] Batch returned null/empty`);
      }

      const fetched = Math.min(start + INDEX_BATCH_SIZE, ids.length);
      if (fetched % 5000 === 0 || fetched === ids.length)
        console.log(`[Index] ${fetched.toLocaleString()} / ${ids.length.toLocaleString()} IDs processed…`);

      if (start + INDEX_BATCH_SIZE < ids.length) await sleep(RATE_DELAY_MS);
    }

    searchIndex = allProjects.map(p => ({
      id           : p.projectId,
      name         : (p.projectName  || '').trim(),
      promoterName : (p.promotorName || '').trim(),
      district     : p.districtName  || '',
      processType  : p.processType   || '',
      projectType  : p.projectType   || '',
      status       : p.projectStatus || '',
    }));
    indexBuiltAt = Date.now();
    cacheWrite('index', searchIndex);
    console.log(`[Index] Done — ${searchIndex.length} projects indexed and cached.`);
  } catch (err) {
    console.error('[Index] Build failed:', err.message);
  } finally {
    indexBuilding = false;
  }
}

function ensureIndex() {
  if (!indexBuiltAt || Date.now() - indexBuiltAt > INDEX_TTL_MS) {
    buildIndex();
  }
}

/* ── HTTP helpers (with 429 auto-retry via retryFetch) ── */
const { retryGet, retryPost } = require('./utils/retryFetch');

async function axiosGet(url, timeout = 12000) {
  return retryGet(url, timeout, HEADERS);
}

async function axiosPost(url, body, timeout = 15000) {
  return retryPost(url, body, timeout, HEADERS);
}

/* ── Unwrap helpers ── */
function unwrap(val, fallback = null) {
  if (val === null || val === undefined) return fallback;
  return (val && val.data !== undefined) ? val.data : val;
}

function unwrapArr(val) {
  if (!val) return [];
  const d = (val && val.data !== undefined) ? val.data : val;
  return Array.isArray(d) ? d : [];
}

/* ── extractArray — handles the many response shapes the RERA API produces ──
   unwrapArr only handles { data: [...] }; RERA also returns { data: { list: [] } },
   { complaintList: [] }, bare arrays, etc.  This function tries them all. */
function extractArray(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  // Standard { data: [...] }
  if (Array.isArray(raw.data)) return raw.data;
  // { data: { someKey: [...] } }
  if (raw.data && typeof raw.data === 'object' && !Array.isArray(raw.data)) {
    for (const key of ['complaintList','applicationList','list','result','records','applications','data']) {
      if (Array.isArray(raw.data[key]) && raw.data[key].length > 0) return raw.data[key];
    }
    // any array value under data
    for (const v of Object.values(raw.data)) {
      if (Array.isArray(v) && v.length > 0) return v;
    }
  }
  // known root-level keys with no data wrapper
  for (const key of ['complaintList','applicationList','list','result','records','applications']) {
    if (Array.isArray(raw[key]) && raw[key].length > 0) return raw[key];
  }
  return [];
}

/* ── Fuzzy search — project name AND promoter name ── */
function scoreText(text, q, words) {
  const t = (text || '').toLowerCase();
  if (!t) return 0;
  if (t === q)                              return 100;
  if (t.startsWith(q))                     return 80;
  if (t.includes(q))                       return 60;
  if (words.every(w => t.includes(w)))     return 40;
  const matched = words.filter(w => t.includes(w)).length;
  // For multi-word queries: partial match only when ≥2 words match.
  // This prevents single common words like "infra", "heights", "residency"
  // from pulling in completely unrelated projects.
  if (words.length >= 2 && matched < 2)    return 0;
  return matched > 0 ? Math.round((matched / words.length) * 30) : 0;
}

function searchByName(query) {
  const q     = query.toLowerCase().replace(/\s+/g, ' ').trim();
  const words = q.split(' ').filter(w => w.length > 1);
  return searchIndex
    .map(p => {
      const nameScore     = scoreText(p.name,         q, words);
      // Promoter name scores at 80% weight so project name always wins ties
      const promoterScore = Math.round(scoreText(p.promoterName, q, words) * 0.8);
      const score         = Math.max(nameScore, promoterScore);
      return { ...p, score };
    })
    .filter(p => p.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 20);
}

/* ── Global search API — handles reg numbers & promoter names (BUG 3 FIX) ── */
const GLOBAL_SEARCH_API = `${RERA_BASE}/project_reg/public/global-search`;
const REG_NO_PATTERN    = /\b(RAA|MAA|CAA)\d+/i;
const PR_PATTERN        = /^PR\//i;

async function globalSearch(query) {
  try {
    const data  = await axiosPost(GLOBAL_SEARCH_API, { query }, 12000);
    const items = data?.data || [];
    console.log(`[GlobalSearch] query="${query}" raw data keys=${JSON.stringify(Object.keys(data||{}))}, items=${items.length}`);
    // Return only PROJECT entities; each has entityId = numeric project ID
    return items
      .filter(i => i.entityType === 'PROJECT' && i.entityId)
      .map(i => ({
        id           : i.entityId,
        name         : (i.entityName || '').trim(),
        promoterName : '',            // not returned by global search
        district     : i.distName   || '',
        projectType  : i.ptype      || '',
        processType  : '',
        status       : '',
        regNo        : i.regNo      || '',
        fromGlobal   : true,
      }));
  } catch {
    return [];
  }
}

/* ── Amenities extraction — formOneB is FLAT, not nested (field = keyYesNo / keyWorkDone) ── */
const AMENITY_FLAT = {
  internalRoad            : 'Internal Road',
  waterSupply             : 'Water Supply',
  sewerage                : 'Sewerage System',
  stormWaterDrains        : 'Storm Water Drains',
  landscaping             : 'Landscaping',
  streetLighting          : 'Street Lighting',
  communityBuildings      : 'Community Buildings',
  treatmentAndDisposal    : 'Sewage Treatment & Disposal',
  solidWasteManagement    : 'Solid Waste Management',
  waterConservation       : 'Water Conservation',
  energyManagement        : 'Energy Management',
  fireProtection          : 'Fire Protection',
  electricalMeterRoom     : 'Electrical Meter Room',
  fireFighting            : 'Fire Fighting System',
  drinkingWaterFacilities : 'Drinking Water',
  emergencyEvacuation     : 'Emergency Evacuation',
  useofRenewableEnergy    : 'Renewable Energy',
  security                : 'Security',
  letterBox               : 'Letter Box',
  compoundWall            : 'Compound Wall',
  plinthProtection        : 'Plinth Protection',
  waterTank               : 'Water Tank',
};

function extractAmenities(formOneB) {
  if (!formOneB) return [];
  return Object.entries(AMENITY_FLAT).map(([key, label]) => {
    const yesNoVal = formOneB[`${key}YesNo`];
    if (yesNoVal === undefined) return null;           // field absent in this form version
    const included = yesNoVal === 'YES' || yesNoVal === 'Y' || yesNoVal === true;
    return {
      key,
      label,
      included,
      workDone  : formOneB[`${key}WorkDone`]          || null,
      startDate : formOneB[`${key}StartActualDate`]   || null,
      endDate   : formOneB[`${key}EndActualDate`]     || null,
    };
  }).filter(Boolean);
}

/* ── Timeline / Delay Detector ── */
/* Compares expected construction progress (derived from planned dates) with the
   actual progress reported in RERA filings. Returns a neutral data-based indicator.
   No accusation or judgement — only data observations.                              */
function calculateTimelineIndicator(opts) {
  const { startDate, endDate, effectiveCompletionDate, overallProgress, extensions, status } = opts;

  // Edge case: project already completed
  if (status && ['completed', 'complete', 'Completed'].includes(status)) {
    return { indicator: 'Completed', label: 'Project reported as completed.', expectedProgress: null, actualProgress: null, progressGap: null };
  }

  // Edge case: no progress data
  if (overallProgress == null) {
    return { indicator: 'Insufficient Data', label: 'Insufficient data to evaluate construction timeline.', expectedProgress: null, actualProgress: overallProgress, progressGap: null };
  }

  // Parse a date string that may be DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, or JS-parseable
  function parseDate(s) {
    if (!s) return null;
    if (typeof s !== 'string') return new Date(s);
    // DD/MM/YYYY or DD-MM-YYYY
    const dmY = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (dmY) return new Date(Date.UTC(+dmY[3], +dmY[2] - 1, +dmY[1]));
    // YYYY-MM-DD
    const Ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (Ymd) return new Date(Date.UTC(+Ymd[1], +Ymd[2] - 1, +Ymd[3]));
    const d = new Date(s);
    return isNaN(d) ? null : d;
  }

  function monthsBetween(a, b) {
    if (!a || !b) return null;
    const da = parseDate(a), db = parseDate(b);
    if (!da || !db || isNaN(da) || isNaN(db)) return null;
    return (db - da) / (1000 * 60 * 60 * 24 * 30.44); // avg days per month
  }

  const today = new Date();

  // Use originalCompletionDate (endDate) for plannedDuration — not the extended date
  const plannedDurationMonths = monthsBetween(startDate, endDate);
  const elapsedMonths         = monthsBetween(startDate, today);

  if (!plannedDurationMonths || plannedDurationMonths <= 0 || !elapsedMonths) {
    return { indicator: 'Insufficient Data', label: 'Insufficient data to evaluate construction timeline.', expectedProgress: null, actualProgress: overallProgress, progressGap: null };
  }

  // Cap elapsedMonths at plannedDuration so expectedProgress maxes at 100%
  const cappedElapsed   = Math.min(elapsedMonths, plannedDurationMonths);
  const expectedProgress = Math.min(100, parseFloat(((cappedElapsed / plannedDurationMonths) * 100).toFixed(1)));
  const actualProgress   = parseFloat(overallProgress);
  const progressGap      = parseFloat((expectedProgress - actualProgress).toFixed(1));

  // Classify
  let indicator, label;
  if (progressGap <= 10) {
    indicator = 'On Track';
    label = 'Reported construction progress is consistent with the timeline implied by the original RERA completion date.';
  } else if (progressGap <= 25) {
    indicator = 'Monitor Progress';
    label = `Reported construction progress (${actualProgress}%) is ${progressGap}% below the timeline implied by the original RERA completion date. Variations may occur due to construction scheduling or approvals.`;
  } else {
    indicator = 'Delay Signal';
    label = `Reported construction progress (${actualProgress}%) is ${progressGap}% below the timeline implied by the original RERA completion date. Further verification recommended.`;
  }

  // Extension metadata (displayed alongside but does not change indicator)
  const extGranted = (extensions || []).length > 0;

  return {
    indicator,
    label,
    expectedProgress,
    actualProgress,
    progressGap,
    plannedDurationMonths : Math.round(plannedDurationMonths),
    elapsedMonths         : Math.round(elapsedMonths),
    extensionGranted      : extGranted,
    extensionCount        : (extensions || []).length,
  };
}

/* ── Risk scoring ── */
function computeRisk(d) {
  const factors = [];

  // 1. Complaints
  const compCount = (d.complaints || []).length;
  if      (compCount === 0)  factors.push({ key: 'complaints', label: 'Complaints',          score: 'green',  note: 'No complaints filed' });
  else if (compCount <= 2)   factors.push({ key: 'complaints', label: 'Complaints',          score: 'yellow', note: `${compCount} complaint(s) on record` });
  else                        factors.push({ key: 'complaints', label: 'Complaints',          score: 'red',    note: `${compCount} complaints recorded in RERA database` });

  // 2. QPR defaults — use qprSummaryTotals (excludes EXEMPT quarters)
  const qprDefaultCount = d.qprDefaultedCount != null
    ? d.qprDefaultedCount
    : (d.qprHistory || []).filter(q => q.isDefault).length;
  const qprTotal = (d.qprHistory || []).filter(q => !q.isExempt).length;
  if      (qprDefaultCount === 0) factors.push({ key: 'qpr', label: 'QPR Compliance',       score: 'green',  note: qprTotal > 0 ? `All ${qprTotal} QPRs filed on time` : 'No QPR history' });
  else if (qprDefaultCount <= 2)  factors.push({ key: 'qpr', label: 'QPR Compliance',       score: 'yellow', note: `${qprDefaultCount} of ${qprTotal} QPRs defaulted` });
  else                             factors.push({ key: 'qpr', label: 'QPR Compliance',       score: 'red',    note: `${qprDefaultCount} of ${qprTotal} QPRs defaulted` });

  // 3. Annual compliance — BUG 3: only count overdue (due date passed), not active/pending
  const annTotal     = (d.annualCompliance || []).length;
  const annSubmitted = (d.annualCompliance || []).filter(a =>  a.submitted).length;
  const annDefaults  = (d.annualCompliance || []).filter(a =>  a.overdue).length;
  const annUpcoming  = (d.annualCompliance || []).filter(a => !a.submitted && !a.overdue);
  if (annDefaults === 0) {
    // Build a note that surfaces upcoming (not-yet-due) filings separately
    let annNote;
    if (annUpcoming.length > 0) {
      const upDue  = annUpcoming[0].dueDate ? new Date(annUpcoming[0].dueDate) : getFilingDueDate(annUpcoming[0].year);
      const upYear = upDue ? upDue.getFullYear() : null;
      const upStr  = upYear ? ` (due Oct ${upYear})` : '';
      annNote = `${annSubmitted} submitted · ${annUpcoming.length} upcoming${upStr}`;
    } else {
      annNote = `${annSubmitted} of ${annTotal} annual filings submitted`;
    }
    factors.push({ key: 'annual', label: 'Annual Filings', score: 'green', note: annNote });
  } else if (annDefaults === 1) {
    factors.push({ key: 'annual', label: 'Annual Filings', score: 'yellow', note: `${annDefaults} annual filing overdue` });
  } else {
    factors.push({ key: 'annual', label: 'Annual Filings', score: 'red',    note: `${annDefaults} annual filings overdue` });
  }

  // 4. Construction progress
  const progress = d.overallProgress ?? 0;
  if      (progress > 75)  factors.push({ key: 'progress', label: 'Construction Progress', score: 'green',  note: `${progress}% complete` });
  else if (progress >= 40) factors.push({ key: 'progress', label: 'Construction Progress', score: 'yellow', note: `${progress}% complete` });
  else                      factors.push({ key: 'progress', label: 'Construction Progress', score: 'red',    note: `${progress}% construction recorded` });

  // 5. Loan exposure — use loanOutstanding from loan details API (actual balance per lender record)
  const netLoan   = parseFloat(d.loanOutstanding || 0);
  const totalCost = parseFloat(d.form3C?.totalEstimatedCost || d.totalEstimatedCost || 0);
  if (netLoan <= 0 && !d.loanOutstandingSource) {
    factors.push({ key: 'loan', label: 'Loan Exposure', score: 'green', note: 'No project loan taken' });
  } else if (netLoan <= 0) {
    factors.push({ key: 'loan', label: 'Loan Exposure', score: 'green', note: 'No outstanding loan balance' });
  } else {
    const pct   = totalCost > 0 ? parseFloat((netLoan / totalCost * 100).toFixed(2)) : null;
    const note  = pct != null
      ? `Loan outstanding: ₹${fmtCrore(netLoan)} (${pct}% of project cost)`
      : `Loan outstanding: ₹${fmtCrore(netLoan)}`;
    const score = pct == null ? 'yellow' : pct > 50 ? 'red' : pct > 25 ? 'yellow' : 'green';
    factors.push({ key: 'loan', label: 'Loan Exposure', score, note });
  }

  // Overall
  let overall = 'green';
  if (factors.some(f => f.score === 'red'))    overall = 'red';
  else if (factors.some(f => f.score === 'yellow')) overall = 'yellow';

  return { overall, factors };
}

/* ── Sales vs Construction Alignment ── */
function computeSalesAlignment(d) {
  // Booking %
  const bBooked = d.unitBookingFromBlocks?.booked ?? parseInt(d.form3A?.bookedUnits || d.unitSummary?.bookedUnits || 0);
  const bTotal  = d.unitBookingFromBlocks?.total  ?? parseInt(d.form3A?.totalUnits  || d.unitSummary?.totalUnits  || 0);
  const bookingPct = bTotal > 0 ? parseFloat((bBooked / bTotal * 100).toFixed(1)) : null;

  const constructionProgress = d.overallProgress ?? 0;

  // Parse formatted date strings (DD/MM/YYYY from fmtDate)
  function parseDate(s) {
    if (!s || s === 'Not Available' || s === 'Pending') return null;
    const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) return new Date(`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`);
    const d2 = new Date(s);
    return isNaN(d2.getTime()) ? null : d2;
  }

  function monthsBetween(a, b) {
    if (!a || !b) return null;
    return (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth());
  }

  const startDate  = parseDate(d.projectStartDate);
  const endDate    = parseDate(d.originalCompletionDate || d.completionDate);
  const today      = new Date();

  const elapsedMonths = monthsBetween(startDate, today);
  const totalMonths   = monthsBetween(startDate, endDate);

  const timeProgress = (elapsedMonths != null && totalMonths != null && totalMonths > 0)
    ? parseFloat((Math.min(100, Math.max(0, elapsedMonths) / totalMonths * 100).toFixed(1)))
    : null;

  const salesVsConstructionGap = bookingPct != null
    ? parseFloat((bookingPct - constructionProgress).toFixed(1)) : null;
  const constructionVsTimeGap  = timeProgress != null
    ? parseFloat((constructionProgress - timeProgress).toFixed(1)) : null;

  // Determine observation (cases evaluated in priority order)
  let observationText = null;

  if (bookingPct !== null) {
    // Case 1: Over-sold — high booking, low construction
    if (bookingPct > 70 && constructionProgress < 50) {
      observationText = 'High booking levels compared to construction progress. Buyers may verify whether construction is keeping pace with sales.';
    }
    // Case 5: Front-loaded sales
    else if (salesVsConstructionGap !== null && salesVsConstructionGap > 30) {
      observationText = 'A significant portion of units are sold compared to construction progress. Buyers may review fund utilisation and project execution pace.';
    }
  }

  // Case 2: Construction lagging behind timeline
  if (!observationText && constructionVsTimeGap !== null && constructionProgress < (timeProgress - 20)) {
    observationText = 'Construction progress appears slower than the project timeline. Review updated timelines and RERA extensions.';
  }

  // Case 4: Low demand signal (only when meaningful time has elapsed)
  if (!observationText && timeProgress != null && timeProgress > 60 && bookingPct !== null && bookingPct < 40) {
    observationText = 'Lower booking levels relative to project timeline. Buyers may evaluate market demand and project traction.';
  }

  // Case 3: Healthy / broadly aligned
  if (!observationText) {
    if (constructionVsTimeGap !== null && Math.abs(constructionVsTimeGap) <= 10) {
      observationText = 'Construction progress is broadly aligned with the project timeline.';
    } else if (constructionVsTimeGap !== null) {
      observationText = constructionVsTimeGap > 0
        ? 'Construction is ahead of the expected project timeline.'
        : 'Construction progress is somewhat behind the project timeline. Buyers may verify the current status on the Gujarat RERA portal.';
    } else {
      observationText = bookingPct !== null
        ? `${bookingPct}% of units are recorded as booked in the RERA database.`
        : null;
    }
  }

  return {
    bookingPercentage    : bookingPct,
    constructionProgress : constructionProgress,
    timeProgress         : timeProgress,
    salesVsConstructionGap,
    constructionVsTimeGap,
    observationText,
  };
}

/* ── Enrich promoter names — only for results that are missing it (BUG 4 FIX) ── */
async function enrichWithPromoterNames(results) {
  // Only hit the API for entries where promoterName is absent — avoids redundant calls
  const needsEnrich = results.filter(r => r.id && !r.promoterName);
  if (needsEnrich.length === 0) return results;   // nothing to do

  const fetched = await Promise.allSettled(
    needsEnrich.map(r => axiosGet(`${PROJECT_API}/${r.id}`, 5000))
  );

  // Build id → promoterName map for safe lookup (avoids indexOf reference issues)
  const enrichMap = new Map();
  needsEnrich.forEach((r, i) => {
    const res  = fetched[i];
    const name = res?.status === 'fulfilled' ? (res.value?.data?.promoterName || null) : null;
    enrichMap.set(r.id, name);
  });

  return results.map(r => ({
    ...r,
    promoterName: r.promoterName || enrichMap.get(r.id) || null,
  }));
}

/* ─────────────────────────────────────────────────────────────────
   Routes
   ───────────────────────────────────────────────────────────────── */

app.get('/health', (req, res) => {
  res.json({
    status      : 'ok',
    service     : 'Gujarat RERA Proxy',
    indexSize   : searchIndex.length,
    indexBuiltAt: indexBuiltAt ? new Date(indexBuiltAt).toISOString() : null,
    indexReady  : indexBuiltAt !== null,
  });
});

app.post('/admin/rebuild-index', (req, res) => {
  if (req.headers['x-admin-key'] !== process.env.ADMIN_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  buildIndex();
  res.json({ message: 'Index rebuild started' });
});

/* ─── Search — project name, promoter name, reg number (BUG 3 FIX) ────────*/
app.get('/api/rera/search', async (req, res) => {
  const query = (req.query.q || '').trim();
  if (!query) return res.status(400).json({ error: 'Missing query' });

  ensureIndex();

  if (searchIndex.length === 0) {
    const globalOnly = await globalSearch(query).catch(() => []);
    console.log(`[Search] globalSearch returned ${globalOnly.length} results for "${query}"`);
    const enriched = await enrichWithPromoterNames(globalOnly).catch(() => globalOnly);
    return res.json({
      indexReady: true,
      query,
      count  : enriched.length,
      results: enriched,
    });
  }

  const isRegQuery = REG_NO_PATTERN.test(query) || PR_PATTERN.test(query);

  // Run local index search AND global search in parallel
  const [localSettled, globalSettled] = await Promise.allSettled([
    Promise.resolve(searchByName(query)),
    globalSearch(query),
  ]);

  const localResults  = localSettled.status  === 'fulfilled' ? localSettled.value  : [];
  const globalResults = globalSettled.status === 'fulfilled' ? globalSettled.value : [];

  // Merge: deduplicate by project ID, local results take priority (have more fields)
  const qLower  = query.toLowerCase().replace(/\s+/g, ' ').trim();
  const qWords  = qLower.split(' ').filter(w => w.length > 1);
  const seenIds = new Set(localResults.map(r => r.id).filter(Boolean));
  const merged  = [...localResults];
  for (const gr of globalResults) {
    if (gr.id && !seenIds.has(gr.id)) {
      if (!isRegQuery) {
        // Re-score global result against local query to filter irrelevant API returns
        const gNameScore     = scoreText(gr.name,         qLower, qWords);
        const gPromoterScore = Math.round(scoreText(gr.promoterName || '', qLower, qWords) * 0.8);
        const gLocalScore    = Math.max(gNameScore, gPromoterScore);
        if (gLocalScore === 0) continue; // drop — no relevance to query
        seenIds.add(gr.id);
        merged.push({ ...gr, score: Math.max(45, gLocalScore) });
      } else {
        seenIds.add(gr.id);
        merged.push({ ...gr, score: 95 });
      }
    }
  }

  if (merged.length === 0) {
    return res.json({ indexReady: true, query, count: 0, results: [] });
  }

  merged.sort((a, b) => (b.score || 0) - (a.score || 0));
  const top20 = merged.slice(0, 20);

  // Enrich only entries missing promoter name (BUG 4 FIX — skips index entries that already have it)
  const enriched = await enrichWithPromoterNames(top20).catch(() => top20);

  res.json({ indexReady: true, query, count: enriched.length, results: enriched });
});

/* ─── Project Detail ─────────────────────────────────────────────*/
app.get('/api/rera/project/:id', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!id || id < 1) return res.status(400).json({ error: 'Invalid project ID' });
  // ?fresh=1 bypasses disk cache — forces a full re-fetch and recompute.
  // Use this after RERA updates a project to see the latest data immediately.
  const bypassCache = req.query.fresh === '1' || req.query.nocache === '1';

  try {
    const data = await fetchFullProject(id, bypassCache);
    if (!data) return res.status(404).json({ error: 'Project not found' });
    data.projectId = id;
    res.json({ found: true, data });
  } catch (err) {
    console.error('[Project Detail]', err.message);
    res.status(502).json({ error: 'Failed to fetch project data', detail: err.message });
  }
});

/* ─── By Registration Number ─────────────────────────────────────*/
app.get('/api/rera/by-regno', async (req, res) => {
  const regNo = (req.query.regno || '').trim().toUpperCase();
  if (!regNo) return res.status(400).json({ error: 'Missing regno parameter' });

  ensureIndex();

  try {
    const candidates = await findProjectByRegNo(regNo);
    if (!candidates) {
      return res.json({ found: false, message: `No project found with registration number: ${regNo}` });
    }
    const data = await fetchFullProject(candidates.id);
    res.json({ found: true, data });
  } catch (err) {
    res.status(502).json({ error: 'Search failed', detail: err.message });
  }
});

/* ═══════════════════════════════════════════════════════════════════
   Legal & Encumbrance Analysis
   ═══════════════════════════════════════════════════════════════════ */

const LEGAL_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;   // 30 days

function legalCacheRead(projectId) {
  try {
    const p    = path.join(CACHE_DIR, `legal_${projectId}.json`);
    const stat = fs.statSync(p);
    if (Date.now() - stat.mtimeMs > LEGAL_CACHE_TTL_MS) return null;
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch { return null; }
}

function legalCacheWrite(projectId, data) {
  try {
    fs.writeFileSync(path.join(CACHE_DIR, `legal_${projectId}.json`), JSON.stringify(data));
  } catch (e) { console.warn('[LegalCache] Write failed:', e.message); }
}

/* Fetch legal document UIDs via the RERA public document API.
   The Angular bundle reveals: GET /project_reg/public/getproject-doc/{id}
   Returns { data: { projectdoc: { encumbranceCertificateDocUId, titleReportUId, ... } } }
   Documents download from: https://gujrera.gujarat.gov.in/vdms/download/{uid}  (no auth needed) */
const VDMS_DOWNLOAD = 'https://gujrera.gujarat.gov.in/vdms/download/';
const PROJECT_DOC_API = `${RERA_BASE}/project_reg/public/getproject-doc/`;

async function scrapeGujreraDocUrls(projectId) {
  const urls = { title_cert: null, encumbrance_cert: null, form3: null };
  try {
    const resp = await axiosGet(`${PROJECT_DOC_API}${projectId}`);
    const pd   = resp?.data?.projectdoc || resp?.projectdoc || {};
    if (pd.encumbranceCertificateDocUId) {
      urls.encumbrance_cert = `${VDMS_DOWNLOAD}${pd.encumbranceCertificateDocUId}`;
    }
    if (pd.titleReportUId) {
      urls.title_cert = `${VDMS_DOWNLOAD}${pd.titleReportUId}`;
    }
    console.log(`[LegalScraper] Project ${projectId} — enc_cert=${!!urls.encumbrance_cert} title=${!!urls.title_cert}`);
  } catch (err) {
    console.warn('[LegalScraper] Could not fetch document URLs:', err.message);
  }
  return urls;
}

/* ── Fetch all available document UIDs for a project ── */
/* fetchProjectDocuments — extracted to utils/fetchProjectDocuments.js (shared with email worker) */
const fetchProjectDocuments = require('./utils/fetchProjectDocuments');

const PDF_MAX_BYTES = 10 * 1024 * 1024; // 10 MB cap — keeps download times reasonable

/* ── PDF text cache — avoids re-downloading same doc on every cold legal request ── */
const PDF_TEXT_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days

function pdfTextCacheKey(pdfUrl) {
  // Use last 40 chars of UID (unique enough, filesystem-safe)
  return 'pdftext_' + pdfUrl.slice(-40).replace(/[^A-Z0-9]/gi, '_');
}

function pdfTextCacheRead(pdfUrl) {
  try {
    const p    = path.join(CACHE_DIR, `${pdfTextCacheKey(pdfUrl)}.txt`);
    const stat = fs.statSync(p);
    if (Date.now() - stat.mtimeMs > PDF_TEXT_CACHE_TTL) return null;
    const txt = fs.readFileSync(p, 'utf8');
    console.log(`[LegalPDF] Text cache hit — ${txt.length} chars`);
    return txt;
  } catch { return null; }
}

function pdfTextCacheWrite(pdfUrl, text) {
  try {
    fs.writeFileSync(path.join(CACHE_DIR, `${pdfTextCacheKey(pdfUrl)}.txt`), text);
  } catch (e) { console.warn('[LegalPDF] Text cache write failed:', e.message); }
}

async function extractPdfText(pdfUrl) {
  if (!pdfUrl) return '';

  // Serve from text cache to avoid re-downloading
  const cached = pdfTextCacheRead(pdfUrl);
  if (cached !== null) return cached;

  try {
    // HEAD check — skip files that are too large
    try {
      const head = await axios.head(pdfUrl, { httpsAgent: SSL_AGENT, headers: HEADERS, timeout: 5000 });
      const cl = parseInt(head.headers['content-length'] || '0');
      if (cl > PDF_MAX_BYTES) {
        console.warn(`[LegalPDF] Skipping ${pdfUrl.slice(-20)}: ${(cl/1024/1024).toFixed(1)} MB exceeds limit`);
        return `[PDF_TOO_LARGE: ${(cl/1024/1024).toFixed(1)} MB]`;
      }
    } catch { /* ignore HEAD failures — proceed with download */ }

    const response = await axios.get(pdfUrl, {
      httpsAgent      : SSL_AGENT,
      headers         : HEADERS,
      timeout         : 30000,            // reduced from 60s → 30s
      responseType    : 'arraybuffer',
      maxContentLength: PDF_MAX_BYTES,
    });
    const buf = Buffer.from(response.data);
    const { PDFParse } = require('pdf-parse');
    const parser = new PDFParse({ data: buf });
    const result = await parser.getText({ data: buf });
    const text = (result?.text || '').trim();
    console.log(`[LegalPDF] Extracted ${text.length} chars from ${pdfUrl.slice(-20)}`);
    if (text.length > 50) pdfTextCacheWrite(pdfUrl, text);  // cache only useful extractions
    return text;
  } catch (err) {
    console.warn('[LegalPDF] Extract failed:', err.message);
    return `[PDF_DOWNLOAD_ERROR: ${err.message}]`;
  }
}

const _LEGAL_SYS = 'You are an expert Indian real estate legal analyst specialising in Gujarat RERA (GujRERA). Analyse the provided document text and return ONLY a valid JSON object with no preamble, no markdown, no explanation. JSON structure must be exactly: { "document_type": "title_certificate | encumbrance_certificate | form3 | title_report", "title_status": "clear | disputed | conditional | unclear | not_applicable", "encumbrance_status": "no_encumbrance | charge_created | partially_released | fully_released | unclear", "litigation_found": true or false, "litigation_details": "string or null", "lender_name": "bank or NBFC name if mortgage found, else null", "advocate_name": "string or null", "certificate_date": "string or null", "key_findings": ["array of plain-English findings, max 4 items"], "buyer_advisory": "1-2 sentence plain-English advisory for a homebuyer", "unit_encumbrance_summary": [{"wing": "string", "units_booked": number, "units_available": number, "encumbrance": "created | released | nil"}] or null, "confidence": "high | medium | low" }. IMPORTANT: encumbrance_status must ALWAYS be one of the five values listed — never use "not_applicable". If the document is a Form 3 CA certificate, use "no_encumbrance" when no loan exists, or "charge_created"/"partially_released"/"fully_released" based on the INFERENCE line in the document.';

const { truncateToLimit, checkContextSize } = require('./utils/contextGuard');

async function analyzeWithClaude(documentType, extractedText) {

  if (!extractedText || extractedText.trim().length < 20) {
    return { error: 'no_text', confidence: 'low' };
  }

  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) return { error: 'missing_api_key', confidence: 'low' };

  // Free model cascade: try primary, then fallback on 429/404
  const MODELS = [
    'nvidia/nemotron-3-super-120b-a12b:free',
    'stepfun/step-3.5-flash:free',
    'google/gemma-3-27b-it:free',
  ];

  let lastErr;
  for (const model of MODELS) {
    try {
      const response = await axios.post(
        "https://openrouter.ai/api/v1/chat/completions",
        {
          model,
          messages: [
            { role: "system", content: _LEGAL_SYS },
            { role: "user", content: `Document type: ${documentType}\n\nDocument text:\n${truncateToLimit(extractedText)}` }
          ]
        },
        {
          headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
          timeout: 90000,
        }
      );

      const text = response.data.choices[0].message.content;
      console.log(`[LegalAI] Model ${model.split('/')[1]} succeeded for ${documentType}`);
      try {
        return JSON.parse(text);
      } catch {
        const m = text.match(/\{[\s\S]*\}/);
        if (m) return JSON.parse(m[0]);
        return { error: "parse_failed", raw: text.slice(0,200) };
      }
    } catch (err) {
      const status = err.response?.status;
      console.warn(`[LegalAI] Model ${model.split('/')[1]} failed (${status}): ${err.message.slice(0,80)}`);
      lastErr = err;
      if (status !== 429 && status !== 404 && status !== 503) break; // Only retry on rate-limit/not-found/unavailable
      await new Promise(r => setTimeout(r, 1000)); // 1s pause between retries
    }
  }
  return { error: lastErr?.message || 'all_models_failed', confidence: 'low' };
}

function generateLegalVerdict(titleResult, encResult, form3Result) {
  titleResult = titleResult || {};
  encResult   = encResult   || {};
  form3Result = form3Result || {};
  let overallRisk = 'low';
  const signals   = [];
  const encStatus = encResult.encumbrance_status || '';
  if (['charge_created', 'partially_released'].includes(encStatus)) {
    overallRisk = 'medium';
    signals.push({ type: 'project_finance', severity: 'medium',
      message     : `Encumbrance certificate shows '${encStatus.replace(/_/g, ' ')}' — a financial charge exists on the project land.`,
      buyer_action: 'Request details of the charge and written confirmation of release before signing any booking agreement.' });
  }
  if (titleResult.litigation_found === true) {
    overallRisk = 'high';
    signals.push({ type: 'litigation', severity: 'high',
      message     : `Litigation found in title certificate: ${titleResult.litigation_details || 'Details not available.'}`,
      buyer_action: 'Consult an independent property lawyer before proceeding with any booking.' });
  }
  // Also check encumbrance certificate for litigation (court case in EC)
  if (encResult.litigation_found === true) {
    overallRisk = 'high';
    signals.push({ type: 'litigation', severity: 'high',
      message     : `⚠️ Active litigation found in encumbrance certificate: ${encResult.litigation_details || 'Details not available.'}`,
      buyer_action: 'A pending court case against this property has been identified. Consult an independent property lawyer before any booking or payment.' });
  }
  const unitSummary = form3Result.unit_encumbrance_summary || [];
  if (unitSummary.some(u => u.encumbrance === 'created')) {
    signals.push({ type: 'unit_encumbrance', severity: 'medium',
      message     : 'One or more wings/units have an active encumbrance as reported in Form 3.',
      buyer_action: 'Verify with the developer that your specific unit is free of encumbrance before making any payment.' });
  }
  const noEnc    = ['no_encumbrance', 'fully_released', 'not_applicable', ''].includes(encStatus);
  const noLit    = !titleResult.litigation_found && !encResult.litigation_found;
  const titClear = titleResult.title_status === 'clear' || titleResult.title_status === 'not_applicable';
  if (noEnc && noLit && titClear && signals.length === 0) {
    signals.push({ type: 'all_clear', severity: 'low',
      message     : 'No encumbrance, no litigation identified in the documents analysed.',
      buyer_action: 'No immediate legal concerns identified. Standard due diligence is still recommended.' });
  }
  return { overall_risk: overallRisk, signals, generated_at: new Date().toISOString() };
}

/* Build a plain-text summary of RERA structured data for Claude analysis when PDFs unavailable */
function buildReraSynthText(projectData, docType) {
  if (!projectData) return '';
  const p = projectData;
  const f3c = p.form3C || {};
  const f3a = p.form3A || {};

  if (docType === 'title_certificate') {
    const landOwners = (p.landOwners || []).map(o =>
      `${o.name || ((o.firstName||'') + ' ' + (o.lastName||'')).trim()} (PAN: ${o.pan||'N/A'}, Type: ${o.promoterType||'N/A'})`
    ).join('; ') || 'N/A';
    const complaints = (p.complaints || []).map(c =>
      `ID: ${c.applicationNo||c.id||'N/A'}, Date: ${c.filedDate||c.approvalDate||'N/A'}`
    ).join('\n') || 'None';
    return [
      `TITLE CERTIFICATE ANALYSIS — ${p.projectName || 'Unknown Project'}`,
      `Registration No: ${p.registrationNo || 'N/A'}`,
      `Promoter: ${p.promoterName || 'N/A'} (${p.promoterType || 'N/A'})`,
      `District: ${p.district || 'N/A'}`,
      `Project Start: ${p.projectStartDate || 'N/A'}`,
      `Approved Date: ${p.approvedDate || 'N/A'}`,
      `Completion Date: ${p.completionDate || 'N/A'}`,
      `Land Owners: ${landOwners}`,
      `Complaints on RERA: ${p.complaintCount || 0}`,
      `Complaint Details:\n${complaints}`,
      `Note: This analysis is derived from Gujarat RERA public records (not a physical title certificate PDF).`,
    ].join('\n');
  }

  if (docType === 'encumbrance_certificate') {
    const loanBalance  = parseFloat(f3c.loanBalance  || 0);
    const loanRepaid   = parseFloat(f3c.loanAmountTaken || 0);
    const netLoan      = Math.max(0, loanBalance - loanRepaid);
    const totalCost    = parseFloat(f3c.totalEstimatedCost || p.totalEstimatedCost || 0);
    const loanExposure = totalCost > 0 ? ((netLoan / totalCost) * 100).toFixed(1) : 'N/A';

    // Derive encumbrance inference from Form 3C loan figures to guide the AI
    let encInference;
    if (loanBalance <= 0) {
      encInference = 'RERA Form 3C shows NO loan taken — encumbrance_status should be "no_encumbrance".';
    } else if (netLoan <= 0) {
      encInference = `RERA Form 3C shows a loan of ₹${loanBalance.toLocaleString()} was taken but has been FULLY REPAID (loanAmountTaken equals loanBalance). A charge was created and has likely been fully discharged — encumbrance_status should be "fully_released".`;
    } else {
      const outPct = totalCost > 0 ? ((netLoan / totalCost) * 100).toFixed(1) : '?';
      encInference = `RERA Form 3C shows an ACTIVE OUTSTANDING LOAN of ₹${netLoan.toLocaleString()} (${outPct}% of project cost). A mortgage/charge is likely in existence — encumbrance_status should be "charge_created".`;
    }

    return [
      `ENCUMBRANCE CERTIFICATE ANALYSIS — ${p.projectName || 'Unknown Project'}`,
      `Registration No: ${p.registrationNo || 'N/A'}`,
      `Total Estimated Project Cost: ₹${totalCost.toLocaleString()}`,
      `Loan Sanctioned (loanBalance): ₹${loanBalance.toLocaleString()}`,
      `Loan Repaid (loanAmountTaken): ₹${loanRepaid.toLocaleString()}`,
      `Net Outstanding Loan: ₹${netLoan.toLocaleString()}`,
      `Loan Exposure % of Total Cost: ${loanExposure}%`,
      // IMPORTANT: escrow bank is the RERA-mandated buyer-collection account (Rule 5), NOT the lender.
      // It must NOT be used as lender_name. The lender (if any) would appear in the PDF document.
      `RERA Escrow Collection Bank (NOT the lender/mortgagee): ${p.bank?.bankName || 'N/A'}`,
      `NOTE: Set lender_name=null unless a mortgage/charge holder is explicitly stated in the document text above.`,
      `Cost Incurred to Date: ₹${parseFloat(f3c.totalCostIncurred || p.totalCostIncurred || 0).toLocaleString()}`,
      `Land Cost: ₹${parseFloat(f3c.landCost || 0).toLocaleString()}`,
      `Development Cost: ₹${parseFloat(f3c.developmentCost || 0).toLocaleString()}`,
      ``,
      `INFERENCE FROM RERA STRUCTURED DATA: ${encInference}`,
      `Note: This analysis is derived from Gujarat RERA Form 3C public records (physical EC PDF was image-based and could not be parsed).`,
    ].join('\n');
  }

  if (docType === 'form3') {
    const collected = parseFloat(f3a.receivedAmount || 0);
    const balance   = parseFloat(f3a.balanceAmount || 0);
    const blocks = (p.blocks || []).map(b =>
      `${b.name}: Total=${b.totalUnits||'?'}, Booked=${b.bookedUnits||'?'}, Available=${b.availableUnits||'?'}, Progress=${b.workDonePercent||'?'}%`
    ).join('\n') || 'N/A';
    return [
      `FORM 3 (CA CERTIFICATE) ANALYSIS — ${p.projectName || 'Unknown Project'}`,
      `Registration No: ${p.registrationNo || 'N/A'}`,
      `Total Units: ${p.totalUnits || 'N/A'}`,
      `Collected from Buyers: ₹${collected.toLocaleString()}`,
      `Balance Receivable: ₹${balance.toLocaleString()}`,
      `Overall Construction Progress: ${p.overallProgress || 0}%`,
      `Block-wise Data:\n${blocks}`,
      `QPR Filing Status: ${p.qprDefaultedCount || 0} defaults out of ${(p.qprHistory||[]).length} QPRs`,
      `Annual Compliance: ${(p.annualCompliance||[]).filter(a=>a.submitted).length} of ${(p.annualCompliance||[]).length} FYs submitted`,
      `Note: This analysis is derived from Gujarat RERA Form 3A/3C public records (not a physical CA certificate PDF).`,
    ].join('\n');
  }

  return '';
}

/* ─────────────────────────────────────────────────────────────────
   Project Health API  — serves QA test results from cache/health/
   GET /api/rera/project/:id/health           → single project
   GET /api/rera/health/summary               → aggregate stats
   GET /api/rera/health/failures?cat=X        → filtered failures
   ───────────────────────────────────────────────────────────────── */

const HEALTH_DIR = path.join(CACHE_DIR, 'health');
if (!fs.existsSync(HEALTH_DIR)) fs.mkdirSync(HEALTH_DIR, { recursive: true });

function readHealthFile(id) {
  try { return JSON.parse(fs.readFileSync(path.join(HEALTH_DIR, `${id}.json`), 'utf8')); }
  catch { return null; }
}

app.get('/api/rera/project/:id/health', (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!id) return res.status(400).json({ error: 'Invalid ID' });
  const h = readHealthFile(id);
  if (!h) return res.status(404).json({ error: 'No health data for this project. Run the QA scan first.', id });
  res.json(h);
});

app.get('/api/rera/health/summary', (req, res) => {
  try {
    const files = fs.readdirSync(HEALTH_DIR).filter(f => /^\d+\.json$/.test(f));
    let pass = 0, warn = 0, fail = 0;
    const catCounts = {};
    const ruleCounts = {};
    files.forEach(f => {
      try {
        const h = JSON.parse(fs.readFileSync(path.join(HEALTH_DIR, f), 'utf8'));
        if      (h.health === 'PASS') pass++;
        else if (h.health === 'WARN') warn++;
        else if (h.health === 'FAIL') fail++;
        (h.issues || []).forEach(i => {
          catCounts[i.category]  = (catCounts[i.category]  || 0) + 1;
          ruleCounts[i.ruleCode] = (ruleCounts[i.ruleCode] || 0) + 1;
        });
      } catch {}
    });
    res.json({
      total: files.length, pass, warn, fail,
      pass_rate: files.length > 0 ? ((pass/files.length)*100).toFixed(1)+'%' : 'N/A',
      issues_by_category: catCounts,
      issues_by_rule: ruleCounts,
      last_updated: fs.statSync(path.join(HEALTH_DIR, files[files.length-1] || '.')).mtime,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/rera/health/failures', (req, res) => {
  const cat = req.query.cat;   // filter by category, optional
  const sev = req.query.sev || 'FAIL';
  try {
    const files = fs.readdirSync(HEALTH_DIR).filter(f => /^\d+\.json$/.test(f));
    const failures = [];
    files.forEach(f => {
      try {
        const h = JSON.parse(fs.readFileSync(path.join(HEALTH_DIR, f), 'utf8'));
        const issues = (h.issues || []).filter(i =>
          i.severity === sev && (!cat || i.category === cat)
        );
        if (issues.length > 0) failures.push({
          id: h.id, name: h.name, approvedDate: h.approvedDate,
          district: h.district, health: h.health, issues,
        });
      } catch {}
    });
    failures.sort((a,b) => b.issues.length - a.issues.length);
    res.json({ count: failures.length, severity: sev, category: cat || 'all', failures });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/rera/project/:id/legal-analysis', async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!id || id < 1) return res.status(400).json({ error: 'Invalid project ID' });

  const cached = legalCacheRead(id);
  if (cached) {
    // Re-compute verdict from cached individual results so verdict logic changes apply without re-analysis
    const freshVerdict = generateLegalVerdict(cached.title, cached.encumbrance, cached.form3);
    return res.json({ ...cached, verdict: freshVerdict, servedFrom: 'cache' });
  }

  try {
    console.log(`[LegalAnalysis] Starting for project ${id}…`);

    // Fetch project data (use disk cache if available) and attempt PDF scrape in parallel
    const [projectResult, pdfUrls] = await Promise.all([
      fetchFullProject(id).catch(() => null),
      scrapeGujreraDocUrls(id),
    ]);
    const projectData = projectResult;
    console.log(`[LegalAnalysis] PDF URLs:`, pdfUrls);

    // Extract text from PDFs — focus on encumbrance certificate only (per-user instruction).
    // Title report is fetched but NOT sent to AI (skipped). Form3 uses RERA structured data.
    // Calls are sequential to avoid rate-limiting on the free OpenRouter tier.
    const encText   = await extractPdfText(pdfUrls.encumbrance_cert);
    // A scanned/image-based PDF yields only page markers like "-- 1 of 10 --".
    // Strip those out; if meaningful content remains (>100 chars), use the PDF text.
    // Otherwise fall back to synthetic RERA data so the AI has real numbers to work from.
    const encTextStripped = (encText || '')
      .replace(/--\s*\d+\s*of\s*\d+\s*--/gi, '')           // remove page markers
      .replace(/scanned\s+by\s+camscanner/gi, '')            // remove scanner watermarks
      .replace(/powered\s+by\s+(adobe|camscanner)/gi, '')    // remove other watermarks
      .replace(/\s+/g, ' ').trim();
    const encTextUseful   = !encText?.startsWith('[PDF') && encTextStripped.length > 100;
    const effectiveEncText = encTextUseful
      ? encText : buildReraSynthText(projectData, 'encumbrance_certificate');

    // Run only encumbrance analysis via AI; derive form3 from RERA data; skip title AI call
    const encResultRaw = await analyzeWithClaude('encumbrance_certificate', effectiveEncText);

    // Post-process: override invalid or "unclear" encumbrance_status using Form 3C facts.
    // AI models sometimes return "not_applicable" (invalid) or "unclear" even when RERA loan
    // data makes the correct status unambiguous. When Form 3C has clear loan figures, override.
    const VALID_ENC_STATUSES = new Set(['no_encumbrance','charge_created','partially_released','fully_released','unclear']);
    const encResult = { ...encResultRaw };
    const _f3cPost  = projectData?.form3C || {};
    const _lbPost   = parseFloat(_f3cPost.loanBalance    || 0);
    const _latPost  = parseFloat(_f3cPost.loanAmountTaken || 0);
    const _netPost  = Math.max(0, _lbPost - _latPost);
    // Override when: (a) invalid enum value, or (b) AI says "unclear" but loan data is conclusive
    const needsOverride = !VALID_ENC_STATUSES.has(encResult.encumbrance_status)
      || (encResult.encumbrance_status === 'unclear' && (_lbPost > 0 || encResult.confidence !== 'high'));
    if (needsOverride && !encTextUseful) {
      // Only override from synthetic fallback (not when real PDF text was used)
      let corrected;
      if (_lbPost <= 0) {
        corrected = 'no_encumbrance';
      } else if (_netPost <= 0) {
        corrected = 'fully_released';
      } else {
        corrected = 'charge_created';
      }
      if (corrected !== encResult.encumbrance_status) {
        console.log(`[LegalPost] Overrode encumbrance_status '${encResultRaw.encumbrance_status}' → '${corrected}' using Form3C data (lb=${_lbPost}, lat=${_latPost})`);
        encResult.encumbrance_status = corrected;
        encResult.confidence = 'medium';
      }
    }

    const form3Text   = buildReraSynthText(projectData, 'form3');
    const form3Result = await analyzeWithClaude('form3', form3Text);
    const titleResult = { title_status: 'not_applicable', confidence: 'low',
                          note: 'Title report analysis skipped — encumbrance-only mode.' };

    const verdict = generateLegalVerdict(titleResult, encResult, form3Result);
    const result  = {
      project_id      : id,
      verdict,
      title           : titleResult,
      encumbrance     : encResult,
      form3           : form3Result,
      pdf_urls        : pdfUrls,
      data_source     : (pdfUrls.title_cert || pdfUrls.encumbrance_cert || pdfUrls.form3)
                          ? 'pdf_and_rera_data' : 'rera_public_records',
      last_analyzed_at: new Date().toISOString(),
      servedFrom      : 'live',
    };
    legalCacheWrite(id, result);
    res.json(result);
  } catch (err) {
    console.error('[LegalAnalysis] Error:', err.message);
    res.status(500).json({ error: err.message, project_id: id });
  }
});

/* ─────────────────────────────────────────────────────────────────
   Document list — returns available document names + count
   ───────────────────────────────────────────────────────────────── */
app.get('/api/rera/project/:id/documents', async (req, res) => {
  const projectId = parseInt(req.params.id);
  if (!projectId) return res.status(400).json({ error: 'Invalid project ID' });
  try {
    const docs = await fetchProjectDocuments(projectId);
    res.json({ projectId, count: docs.length, documents: docs.map(d => ({ uid: d.uid, label: d.label, filename: d.filename })) });
  } catch (err) {
    res.status(502).json({ error: 'Failed to fetch document list', detail: err.message });
  }
});


/* ─────────────────────────────────────────────────────────────────
   VDMS pass-through — lets the browser fetch individual PDFs via
   our SSL agent (RERA uses legacy TLS that browsers reject directly).
   Used by the client-side JSZip downloader.
   GET /api/rera/vdms/:uid  → streams the PDF from RERA VDMS.
   ───────────────────────────────────────────────────────────────── */
app.get('/api/rera/vdms/:uid', async (req, res) => {
  const uid = req.params.uid;
  // Basic safety check — allow base64 chars, URL-encoded chars are decoded by Express
  if (!uid || uid.length < 5 || uid.length > 300) {
    return res.status(400).end();
  }
  try {
    const upstream = await axios.get(`${VDMS_DOWNLOAD}${uid}`, {
      httpsAgent  : SSL_AGENT,
      responseType: 'stream',
      timeout     : 90_000,
      headers     : {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer'   : 'https://gujrera.gujarat.gov.in/',
        'Accept'    : 'application/pdf,*/*',
      },
    });
    const ct = upstream.headers['content-type'] || 'application/pdf';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'private, max-age=3600');
    if (upstream.headers['content-length']) {
      res.setHeader('Content-Length', upstream.headers['content-length']);
    }
    upstream.data.pipe(res);
    upstream.data.on('error', () => { if (!res.headersSent) res.status(502).end(); });
    res.on('close', () => upstream.data.destroy());
  } catch (e) {
    console.warn(`[VDMS] uid=${uid.slice(0,20)} error: ${e.message}`);
    if (!res.headersSent) res.status(502).end();
  }
});

/* ─────────────────────────────────────────────────────────────────
   Document download — streams all docs as in-memory ZIP (no disk write)
   ───────────────────────────────────────────────────────────────── */
app.get('/api/rera/project/:id/download-documents', async (req, res) => {
  const projectId = parseInt(req.params.id);
  if (!projectId) return res.status(400).json({ error: 'Invalid project ID' });

  // Rate limiting: 1 download per IP per 10 seconds
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';
  if (!checkDownloadRateLimit(clientIp)) {
    return res.status(429).json({ error: 'Too many requests. Please wait 10 seconds before downloading again.' });
  }

  try {
    const docs = await fetchProjectDocuments(projectId);
    if (docs.length === 0) {
      return res.status(404).json({ error: 'No documents found for this project.' });
    }

    const archiver = require('archiver');
    const archive  = archiver('zip', { zlib: { level: 5 } });

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="RERA_Project_${projectId}_Documents.zip"`);
    res.setHeader('X-Doc-Count', docs.length);

    archive.pipe(res);

    archive.on('error', err => {
      console.error('[DocZip] Archive error:', err.message);
      if (!res.headersSent) res.status(500).json({ error: 'Archive failed' });
    });

    // Fetch each document as a buffer (not stream) so timeout applies to full body.
    // Concurrency limited to 5 simultaneous VDMS connections to avoid overwhelming the server.
    const vdmsAgent = new (require('https').Agent)({
      rejectUnauthorized: false,
      secureOptions: require('constants').SSL_OP_LEGACY_SERVER_CONNECT,
    });
    const vdmsLimit = require('p-limit')(5);
    const DOC_TIMEOUT_MS = 60_000;  // 60s hard timeout per document (body included)

    let fetched = 0;
    await Promise.allSettled(docs.map(doc => vdmsLimit(async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), DOC_TIMEOUT_MS);
      try {
        const resp = await axios.get(doc.url, {
          httpsAgent     : vdmsAgent,
          responseType   : 'arraybuffer',
          signal         : controller.signal,
          maxContentLength: 20 * 1024 * 1024,  // 20 MB per-doc cap
          headers        : { 'User-Agent': 'Mozilla/5.0 (compatible; GujreraExperts/1.0)' },
        });
        clearTimeout(timer);
        archive.append(Buffer.from(resp.data), { name: doc.filename });
        fetched++;
      } catch (err) {
        clearTimeout(timer);
        console.warn(`[DocZip] Skipped ${doc.filename}:`, err.message);
        // Append a placeholder text file so the user knows this doc was unavailable
        archive.append(`Document not available: ${doc.label}\n${doc.url}`, { name: doc.filename.replace(/\.pdf$/, '_UNAVAILABLE.txt') });
      }
    })));

    console.log(`[DocZip] Project ${projectId} — ${fetched}/${docs.length} docs fetched, finalizing zip`);
    await archive.finalize();

  } catch (err) {
    console.error('[DocZip] Error:', err.message);
    if (!res.headersSent) res.status(502).json({ error: 'Download failed', detail: err.message });
  }
});

/* ─────────────────────────────────────────────────────────────────
   Core: Fetch full project data from all APIs
   ───────────────────────────────────────────────────────────────── */
async function fetchFullProject(projectId, bypassCache = false) {
  /* ── Serve from disk cache if fresh ── */
  const cached = bypassCache ? null : cacheRead(projectId);
  if (cached) {
    cached.servedFrom = 'cache';
    return cached;
  }

  /* Stage 1: Core project data */
  const core = await axiosGet(`${PROJECT_API}/${projectId}`).catch(() => null);
  if (!core?.data) return null;
  const c = core.data;

  const formOneId = c.formOneId;
  const formTwoId = c.formTwoId;

  /* Extract promoter ID for group entity + possession APIs */
  const promoterId = c.promoterId || c.promotorId || null;

  /* Stage 2: All parallel fetches */
  const [
    mapResult,
    form1Result,
    form2Result,
    form1FullResult,
    qprFormIdsResult,
    banksResult,
    landResult,
    prevProjectsResult,
    promoterDetResult,
    unitDetailsResult,
    blockChartResult,
    qprHistoryResult,
    annualCompResult,
    projectAppsResult,
    promoterGroupResult,
    possessionResult,
    projectDocResult,
    bankWithdrawalResult,
    qeReturnResult,
  ] = await Promise.allSettled([
    axiosPost(MAP_API, [projectId]),
    axiosGet(`${FORM1_API}/${projectId}`),
    formTwoId   ? axiosGet(`${FORM2_API}/${formTwoId}`)                          : Promise.resolve(null),
    formOneId   ? axiosGet(`${FORM1_FULL_API}/${formOneId}`)                     : Promise.resolve(null),
    axiosGet(`${QUARTER_API}/get-qtr-form-details/${projectId}`),
    axiosGet(`${BANKS_API}/${projectId}`),
    axiosGet(`${LAND_API}?projectId=${projectId}`),
    axiosGet(`${PREV_PROJECTS_API}/${projectId}`),
    axiosGet(`${PROMOTER_DET_API}/${projectId}`),
    axiosGet(`${DASHBOARD_API}/get-all-view-unit-details-by-id/${projectId}`),
    axiosGet(`${DASHBOARD_API}/get-all-block-chart-details-by-id/${projectId}`),
    axiosGet(`${QUARTER_API}/getprojectqtrs/${projectId}`),
    axiosGet(`${FORM5_API}/get-formfive-records-till-date/${projectId}`),
    axiosGet(`${PROJECT_APPS_API}/${projectId}`).catch(() => null),
    promoterId ? axiosGet(`${PROMOTER_GROUP_API}${promoterId}`).catch(() => null)            : Promise.resolve(null),
    promoterId ? axiosGet(`${POSSESSION_API}/${projectId}/${promoterId}`).catch(() => null)  : Promise.resolve(null),
    axiosGet(`${PROJECT_DOC_API}${projectId}`).catch(() => null),
    axiosGet(`${BANKS_WITHDRAWAL_API}/${projectId}`).catch(() => null),
    axiosGet(`${QE_RETURN_API}/${projectId}`).catch(() => null),
  ]);

  const mapData        = mapResult.status        === 'fulfilled' ? mapResult.value?.data?.[0]   : null;
  const form1          = form1Result.status       === 'fulfilled' ? unwrap(form1Result.value)     : null;
  const form2          = form2Result.status       === 'fulfilled' ? form2Result.value             : null;
  const form1Full      = form1FullResult.status   === 'fulfilled' ? unwrap(form1FullResult.value) : null;
  const qprFormIdsRaw  = qprFormIdsResult.status  === 'fulfilled' ? qprFormIdsResult.value        : null;
  const banksRaw       = banksResult.status       === 'fulfilled' ? banksResult.value             : null;
  const landRaw        = landResult.status        === 'fulfilled' ? landResult.value              : null;
  const prevProjRaw    = prevProjectsResult.status === 'fulfilled' ? prevProjectsResult.value     : null;
  const promoterDetRaw = promoterDetResult.status  === 'fulfilled' ? promoterDetResult.value      : null;
  const unitRaw        = unitDetailsResult.status  === 'fulfilled' ? unitDetailsResult.value      : null;
  const blockChartRaw  = blockChartResult.status   === 'fulfilled' ? blockChartResult.value       : null;
  const qprHistoryRaw  = qprHistoryResult.status   === 'fulfilled' ? qprHistoryResult.value       : null;
  const annualRaw        = annualCompResult.status    === 'fulfilled' ? annualCompResult.value    : null;
  const projectAppsRaw   = projectAppsResult.status   === 'fulfilled' ? projectAppsResult.value   : null;
  const promoterGroupRaw = promoterGroupResult.status === 'fulfilled' ? promoterGroupResult.value : null;
  const possessionRaw    = possessionResult.status    === 'fulfilled' ? possessionResult.value    : null;
  const projectDocRaw        = projectDocResult.status        === 'fulfilled' ? projectDocResult.value        : null;
  const bankWithdrawalRaw    = bankWithdrawalResult.status    === 'fulfilled' ? bankWithdrawalResult.value    : null;
  // Q-E return: API returns { status:"200", data:{...} } when filed, { status:"300", data:null } when not filed
  const qeReturnRaw          = qeReturnResult.status          === 'fulfilled' ? qeReturnResult.value?.data    : null;
  const _qeReturnFiled       = qeReturnRaw?.status === 'SUBMITTED';
  if (_qeReturnFiled) console.log(`[QEReturn] ${projectId} raw:`, JSON.stringify(qeReturnRaw));

  /* Stage 3: form3 + BUG 4 FIX: block progress from dashboard using QPR's formOneId */
  const qprFormIds   = unwrap(qprFormIdsRaw, qprFormIdsRaw);

  // Q-E quarter lookup ID — used in Stage 3 to resolve the Q-E Form 1 formOneId.
  // Strategy: RERA's get-qtr-form-details endpoint accepts a quarter record ID (qusrterId) and returns
  // { formOneId, formThreeId } for that quarter. For Q-E specifically, the correct qusrterId comes from
  // the Q-E entry in getprojectqtrs (qprHistoryRaw). When that entry exists, its qusrterId is reliable.
  // Fallback: qeReturnRaw.ret_id (the Q-E return record ID). For some projects this equals the qusrterId
  // and works; for others (like RAA11283 where Q-E is NOT in getprojectqtrs) it does not.
  const _qeQusrterId = (() => {
    const arr = unwrapArr(qprHistoryRaw);
    const qe  = arr.find(q => {
      const u = (q.quarterName || '').replace(/[-\s]/g, '').toUpperCase();
      return u === 'QE' || u.startsWith('QUARTEREND');
    });
    return qe?.qusrterId ? String(qe.qusrterId) : null;
  })();
  const _qeQuarterLookupId = _qeQusrterId || (qeReturnRaw?.ret_id ? String(qeReturnRaw.ret_id) : null);
  console.log(`[QEForm1] ${projectId} qusrterId-from-qprHistory=${_qeQusrterId} ret_id=${qeReturnRaw?.ret_id} → using=${_qeQuarterLookupId || 'none'}`);
  console.log(`[QPRFormIds] ${projectId} raw:`, JSON.stringify(qprFormIdsRaw));
  // Initial candidate — overridden below after date comparison selects the true latest certificate.
  // Do NOT use this directly for Form 3A/3C fetches; use formThreeId after it is reassigned.
  let formThreeId  = qprFormIds?.formThreeId || c.formThreeId;
  // The QPR formOneId is the key that unlocks precise block progress from the dashboard
  const qprFormOneId = qprFormIds?.formOneId;

  // ── Loan formThreeId selection ────────────────────────────────────────────────────────────────
  // RERA creates a new Form 3 (CA certificate / Form 8) for EACH application type:
  //   - QPR filings     → qprFormIds.formThreeId  (from get-qtr-form-details)
  //   - Alteration/Reg  → c.formThreeId           (RERA updates alldatabyprojectid after approval)
  //
  // Strategy: compare the latest QPR end-date vs latest Alteration/Registration approval_date.
  // Whichever application type is more recent → use that formThreeId for loan details.
  // This ensures we always read the most recently certified CA certificate.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  // Local date parser — parseDate() is only scoped inside other functions, so define one here
  const _pd2 = (s) => {
    if (!s) return null;
    if (typeof s !== 'string') { const d = new Date(s); return isNaN(d) ? null : d; }
    const dmY = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (dmY) return new Date(Date.UTC(+dmY[3], +dmY[2]-1, +dmY[1]));
    const Ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (Ymd) return new Date(Date.UTC(+Ymd[1], +Ymd[2]-1, +Ymd[3]));
    const d = new Date(s); return isNaN(d) ? null : d;
  };

  // Latest non-BWA QPR end date
  const _qprRows = extractArray(qprHistoryRaw).filter(q => !((q.quarterName || '').startsWith('BWA')));
  const _qprDates = _qprRows.map(q => _pd2(q.endDate || q.startDate || '')).filter(Boolean);
  const latestQprDate = _qprDates.length > 0 ? new Date(Math.max(..._qprDates)) : new Date(0);

  // ── Latest certificate date selection ────────────────────────────────────────────────────────
  // RERA creates a new Form 3 (CA certificate / Form 8) for EACH application type that is approved:
  //   Registration   → updates c.formOneId + c.formThreeId
  //   Alteration     → updates c.formOneId + c.formThreeId
  //   Extension      → updates c.formOneId + c.formThreeId
  //   Bank Withdrawal→ updates c.formOneId + c.formThreeId (promoter submits latest Form 1 with BWA)
  //   QPR            → qprFormIds.formOneId + qprFormIds.formThreeId (separate IDs, not c.*Id)
  //
  // ALL non-QPR, non-complaint approved applications update BOTH Form 1 and Form 3.
  // A single date variable (latestCertDate) governs both Form 1 and Form 3 data source selection.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  // All approved non-QPR, non-complaint rows from projectAllApplications
  const _allNonQprRows = extractArray(projectAppsRaw).filter(a => {
    const t = (a.type || '').toUpperCase().trim();
    if (!a.approval_date) return false;                        // must be approved
    if (t.includes('COMPLAINT') || t.includes('CMP')) return false; // complaints don't file forms
    if (t === 'Q-E' || t === 'QE' || t.startsWith('Q-')) return false; // QPR types
    return true;
  });

  // Bank Withdrawal approval dates from BANKS_WITHDRAWAL_API (separate endpoint, not in projectAppsRaw)
  const _bwaAppRows = extractArray(bankWithdrawalRaw);
  const _bwaDates   = _bwaAppRows
    .map(b => _pd2(b.approvalDate || b.approval_date || b.approvedDate || b.wdrlApprovalDate || b.sanctionDate || ''))
    .filter(Boolean);

  // latestCertDate: most recent certificate filing from ANY non-QPR application type
  // Governs BOTH Form 1 (construction %, booking) and Form 3 (financials, loans, units) selection.
  const _certDates = [
    ..._allNonQprRows.map(a => _pd2(a.approval_date)).filter(Boolean),
    ..._bwaDates,
  ];
  const latestCertDate = _certDates.length > 0 ? new Date(Math.max(..._certDates)) : new Date(0);

  // latestF1Date = latestCertDate: Bank Withdrawal also requires promoter to submit latest Form 1,
  // so ALL non-QPR application types update Form 1 — no separate date tracking needed.
  const latestF1Date  = latestCertDate;

  // Backward-compat alias used in logging and Form 1 comparisons below
  const latestAltDate = latestF1Date;

  // ── Form 3 (CA certificate) winner selection ──────────────────────────────────────────────────
  // Compare latestCertDate (any non-QPR application, incl. Bank Withdrawal) vs latestQprDate.
  // Whichever is more recent → use that formThreeId for ALL Form 3 data.
  let loanDetailsFormThreeId;
  let loanOutstandingSource;

  if (latestCertDate > latestQprDate && c.formThreeId) {
    // A non-QPR application (Alteration / Extension / Bank Withdrawal / Registration) was approved
    // more recently than the latest QPR → RERA has updated c.formThreeId to that certificate.
    loanDetailsFormThreeId = c.formThreeId;
    const latestCertApp = _allNonQprRows
      .sort((a, b) => (_pd2(b.approval_date) || 0) - (_pd2(a.approval_date) || 0))[0];
    const dateLabel = latestCertApp?.approval_date ? fmtDate(latestCertApp.approval_date) : 'date unknown';
    loanOutstandingSource = `${latestCertApp?.type || 'Non-QPR'} CA Certificate (approved ${dateLabel})`;
  } else {
    // Latest QPR is more recent (or no non-QPR application) → use QPR's Form 3
    loanDetailsFormThreeId = qprFormIds?.formThreeId || c.formThreeId;
    loanOutstandingSource  = qprFormIds?.formThreeId ? 'QPR CA Certificate' : 'CA Certificate';
  }

  // ── Single formThreeId for ALL Form 3 data (thumb rule: latest cert date wins) ────────────────
  // loanDetailsFormThreeId is the date-comparison winner — whichever of QPR or non-QPR application
  // (Alteration / Extension / Registration / Bank Withdrawal) was approved more recently.
  // ALL Form 3 data — unit counts (Form 3A), fund flow, financials (Form 3C), loan details —
  // come from this single latest certificate.
  formThreeId = loanDetailsFormThreeId;

  // latestF1Date === latestCertDate (unified — all non-QPR types update both Form 1 and Form 3)
  console.log(`[CertSelect] ${projectId} | QPR=${latestQprDate.toISOString().slice(0,10)} LatestCert=${latestCertDate.toISOString().slice(0,10)} | c.f3=${c.formThreeId} qpr.f3=${qprFormIds?.formThreeId} | winner=${formThreeId} src="${loanOutstandingSource}"`);
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  // QPR Form 3 ID — for unit counts (see explanation below)
  const qprFormThreeId = qprFormIds?.formThreeId;

  const stagePromises = [
    // Form 3A: latest cert (financials, fund flow)
    formThreeId            ? axiosGet(`${FORM3_API}/get-fromthree-a-details-byid/${formThreeId}`)                           : Promise.resolve(null),
    // Form 3C: latest cert (financials: total cost, cost incurred, loan amount)
    loanDetailsFormThreeId ? axiosGet(`${FORM3_API}/get-fromthree-c-details-byid/${loanDetailsFormThreeId}`).catch(() => null) : Promise.resolve(null),
    // BUG 4: fetch precise block progress via QPR formOneId → dashboard endpoint
    qprFormOneId ? axiosGet(`${RERA_BASE}/dashboard/project-block-progress-dtl-by-prj-id/${qprFormOneId}`)       : Promise.resolve(null),
    // QPR form-one full data: has latest noOfUnitsBooked per block (more current than block chart)
    qprFormOneId ? axiosGet(`${FORM1_FULL_API}/${qprFormOneId}`).catch(() => null)                               : Promise.resolve(null),
    // Loan details: per-lender records; loanBalance per record = actual outstanding (loanDisbursalReceived − loanRepaid)
    loanDetailsFormThreeId ? axiosGet(`${FORM3_API}/getform-three-loan-details/${loanDetailsFormThreeId}`).catch(() => null) : Promise.resolve(null),
    // QPR Form 3A — for unit counts ONLY. When an alteration is approved, RERA updates c.formThreeId
    // (Form 3C financials are current) but Form 3A numberOfUnits on that cert can still carry the
    // pre-alteration count. The promoter declares the correct post-alteration unit count in their
    // QPR Form 3A submission. Fetched only when QPR cert differs from latest cert.
    (qprFormThreeId && qprFormThreeId !== formThreeId)
      ? axiosGet(`${FORM3_API}/get-fromthree-a-details-byid/${qprFormThreeId}`).catch(() => null)
      : Promise.resolve(null),
    // Q-E Form 1 — two-step lookup:
    // Step 1: get-qtr-form-details/${_qeQuarterLookupId} → { formOneId, formThreeId } for the Q-E quarter.
    //         Primary: qusrterId from getprojectqtrs Q-E entry (most reliable — same type of ID used for QPR).
    //         Fallback: qeReturnRaw.ret_id (works for projects where Q-E qusrterId matches ret_id).
    //         For Registration-only projects where Q-E is not in getprojectqtrs (e.g. RAA11283),
    //         ret_id is not a valid qusrterId and this step returns null → no Form 1 fetched.
    // Step 2: FORM1_FULL_API/${qeF1Id} → Q-E Form 1 with per-block buDate, buNumber, buDocUId.
    (_qeReturnFiled && _qeQuarterLookupId)
      ? axiosGet(`${QUARTER_API}/get-qtr-form-details/${_qeQuarterLookupId}`)
          .then(qeQtrRaw => {
            const qeQtrIds = unwrap(qeQtrRaw, qeQtrRaw);
            const qeF1Id   = qeQtrIds?.formOneId;
            console.log(`[QEForm1] ${projectId} get-qtr-form-details(${_qeQuarterLookupId}) → formOneId=${qeF1Id} raw=${JSON.stringify(qeQtrIds).slice(0, 120)}`);
            if (!qeF1Id) return null;
            return axiosGet(`${FORM1_FULL_API}/${qeF1Id}`).catch(e => {
              console.log(`[QEForm1] ${projectId} FORM1_FULL(${qeF1Id}) FAILED status=${e?.response?.status || 'no-status'} msg=${(e?.message || '').slice(0, 80)}`);
              return null;
            });
          })
          .catch(e => {
            console.log(`[QEForm1] ${projectId} get-qtr-form-details(${_qeQuarterLookupId}) FAILED status=${e?.response?.status || 'no-status'} msg=${(e?.message || '').slice(0, 80)}`);
            return null;
          })
      : Promise.resolve(null),
  ];
  const [f3aRes, f3cRes, blockProgressRes, qprForm1FullRes, loanDetailsRes, qprF3aRes, qeForm1RetIdRes] = await Promise.allSettled(stagePromises);

  let form3A     = f3aRes.status    === 'fulfilled' ? unwrap(f3aRes.value)    : null;
  let form3C     = f3cRes.status    === 'fulfilled' ? unwrap(f3cRes.value)    : null;
  // QPR Form 3A — unit count source when QPR cert ≠ latest cert
  const qprForm3A    = qprF3aRes?.status === 'fulfilled' ? unwrap(qprF3aRes.value) : null;
  // fund flow uses latest cert Form 3A (same as financials)
  const latestForm3A = null; // slot kept for compat; unused
  const qprBlockProgressArr = blockProgressRes.status === 'fulfilled'
    ? unwrapArr(blockProgressRes.value)
    : [];
  // QPR form-one: noOfUnitsBooked per block (QPR submission)
  const qprForm1Full = qprForm1FullRes?.status === 'fulfilled' ? unwrap(qprForm1FullRes.value) : null;
  // Q-E Form 1: validate projectId matches before using — prevents cross-project data pollution
  const _qeF1RetId = unwrap(qeForm1RetIdRes?.status === 'fulfilled' ? qeForm1RetIdRes.value : null);
  const qeForm1Full = (_qeF1RetId?.formOneAList?.length && String(_qeF1RetId.projectId) === String(projectId))
    ? _qeF1RetId
    : null;
  console.log(`[QEForm1] ${projectId} ret_id=${qeReturnRaw?.ret_id} returned_projectId=${_qeF1RetId?.projectId ?? 'null'} valid=${qeForm1Full ? 'YES' : 'NO'}`);

  // Block name normalizer — strips spaces around + and - so "D + E + F + G" === "D+E+F+G".
  // RERA's two APIs (formOneAList and block chart) can return the same block with different
  // internal spacing; UPPERCASE + TRIM alone is not enough.
  const _normKey = s => (s || '').toUpperCase()
    .replace(/\s*\+\s*/g, '+')
    .replace(/\s*-\s*/g, '-')
    .trim();

  // Per-block booking map.
  // noOfUnitsBooked is reliably populated only in QPR Form 1 submissions.
  // When an alteration is more recent than the QPR, we PREFER form1Full — but if its
  // formOneAList entries all have noOfUnitsBooked = null (alteration did not re-fill
  // that field), fall back to qprForm1Full so we still get a map rather than an empty one.
  const _f1BookingPreferred = (latestF1Date > latestQprDate)
    ? (form1Full?.formOneAList || [])
    : (qprForm1Full?.formOneAList || []);
  const _preferredHasBooking = _f1BookingPreferred.some(b => b.noOfUnitsBooked != null);
  const _blockBookingSrc     = _preferredHasBooking
    ? _f1BookingPreferred
    : (qprForm1Full?.formOneAList || []);
  const _bookingSrcLabel = _preferredHasBooking
    ? (latestF1Date > latestQprDate ? 'form1Full' : 'qprForm1Full')
    : 'qprForm1Full(fallback-no-data-in-preferred)';

  const latestBookingMap = new Map(
    _blockBookingSrc
      .filter(b => b.noOfUnitsBooked != null)
      .map(b => [_normKey(b.blockName), parseInt(b.noOfUnitsBooked)])
  );
  // Diagnostic — shows raw booking data from both Form 1 sources so key mismatches are visible.
  // Check these logs when per-block booking counts look wrong.
  console.log(`[BookingMap] ${projectId} | src=${_bookingSrcLabel} | mapSize=${latestBookingMap.size} | map=${JSON.stringify(Object.fromEntries(latestBookingMap))}`);
  console.log(`[BookingMap] ${projectId} | form1Full blocks=[${(form1Full?.formOneAList||[]).map(b=>`${b.blockName}:${b.noOfUnitsBooked}`).join(', ')}]`);
  console.log(`[BookingMap] ${projectId} | qprForm1Full blocks=[${(qprForm1Full?.formOneAList||[]).map(b=>`${b.blockName}:${b.noOfUnitsBooked}`).join(', ')}]`);

  // Keep qprBookingMap for backward-compat references; point it at the unified map
  const qprBookingMap = latestBookingMap;

  // Loan details: per-lender records from Form 3 loan section.
  // loanBalance per record = loanDisbursalReceived − loanRepaid = actual outstanding per lender.
  // Sum of all loanBalance values = total loan outstanding (matches certificate details page).
  const loanDetailsArr  = (() => {
    const raw = loanDetailsRes?.status === 'fulfilled' ? loanDetailsRes.value : null;
    if (!raw) return [];
    const arr = Array.isArray(raw?.data) ? raw.data : Array.isArray(raw) ? raw : [];
    return arr;
  })();
  const loanOutstanding = loanDetailsArr.reduce((s, l) => s + parseFloat(l.loanBalance || 0), 0);

  /* ── Process data ── */
  const projectName = (c.projectName || mapData?.projectName || 'Unknown').trim();

  // ── Complaints — dual-source with robust extraction ──────────────────────────
  // Source 1: projectAllApplications  (scoped to this project, proper public IDs)
  // Source 2: filter-hearing-details  (fallback; returns hearing rows with case IDs)
  // Both use extractArray() which handles the many response shapes the API returns.

  // Normalise a complaint row from projectAllApplications.
  // Actual API fields (confirmed by live call): application_number, approval_date, type
  function toComplaintRow(a) {
    const id = (a.application_number || a.aknowledgementNo || a.applicationNo ||
                a.comp_no || a.applicationId || '').toString().trim();
    // Keep explicit complaint rows even when no application number is present — generate fallback ID
    const resolvedId = id || (a.type ? `COMP-${Date.now()}-${Math.random().toString(36).slice(2,7)}` : null);
    if (!resolvedId) return null;
    return {
      comp_no  : resolvedId,
      type     : (a.type || a.compType || a.applicationType || 'Complaint').toString(),
      filedDate: fmtDate(a.approval_date   || a.submissionDate || a.filedDate ||
                         a.applicationDate || a.created_date   || null),
      status   : a.applicationStatus || a.status || a.caseStatus || null,
    };
  }

  // Use projectAllApplications — filter to complaint rows only (type "Complaint"),
  // then normalise. Registration/Extension rows in the same array are excluded.
  const complaints = extractArray(projectAppsRaw)
    .filter(a => {
      const t = (a.type || a.compType || a.applicationType || '').toString();
      const id = (a.application_number || a.aknowledgementNo || '').toString();
      return t === 'Complaint' || t === 'SMC' ||
             /^(CMP|SMC)\//i.test(id);
    })
    .map(toComplaintRow)
    .filter(Boolean);

  // Debug: warn when raw data arrived but extraction yielded nothing (helps diagnose new API shapes)
  if (complaints.length === 0 && projectAppsRaw) {
    console.warn(
      `[Complaints] project ${projectId}: 0 extracted. apps sample:`,
      JSON.stringify(projectAppsRaw)?.slice(0, 600)
    );
  }

  // Additional application flags (Form 4, Revenue Record, etc.)
  const allAppsArr        = extractArray(projectAppsRaw);
  const hasForm4          = allAppsArr.some(a => a.type === 'Form4' || a.type === 'Form 4');

  // Project-doc fields (getproject-doc API) — authoritative source for handover documents
  const _pd = projectDocRaw?.data?.projectdoc || projectDocRaw?.projectdoc || {};
  console.log(`[ProjectDoc] ${projectId} keys:`, Object.keys(_pd).join(', ') || '(none)');
  console.log(`[ProjectDoc] ${projectId} buCertificateUId=${_pd.buCertificateUId} propertyCardUId=${_pd.propertyCardUId} societyAppDocUId=${_pd.societyAppDocUId} ersPromaffidavitDocUId=${_pd.ersPromaffidavitDocUId}`);
  // BU Certificate: actual RERA API field is buCertificateUId; older registrations may use buPdfDocUId etc.
  const hasBuildingPermissionDoc = !!(_pd.buCertificateUId || _pd.buPdfDocUId || _pd.buAppDocUId || _pd.buildingPermissionDocUId || _pd.bpDocUId);
  // Revenue Record / Property Card: check all three upload slots + legacy flag
  const hasRevenueRecord = !!(
    _pd.propertyCardUId || _pd.propertyCard2UId || _pd.propertyCard3UId ||
    (_pd.propertyCardIdFlag && _pd.propertyCardIdFlag !== 'No' && _pd.propertyCardIdFlag !== 'NO' && _pd.propertyCardIdFlag !== '0')
  );
  const hasSocietyFromApps       = !!(_pd.societyAppDocUId) || !!(_qeReturnFiled && qeReturnRaw?.qt_society_reg_docuid) || allAppsArr.some(a => {
    const t = (a.type || '').toLowerCase();
    return t.includes('society') || t.includes('incorporation');
  });
  const hasAffidavit             = !!(_pd.ersPromaffidavitDocUId || _pd.intendPromaffidavitDocUId || (_qeReturnFiled && qeReturnRaw?.qt_addidavit_docuid));

  // Extensions — each entry with type "Extension" in projectAllApplications
  // Application number format (newer): .../EXn/{approvalDDMMYY}/{validityDDMMYY}
  // The validity segment is the new project completion date granted by RERA.
  const extensions = allAppsArr
    .filter(a => a.type === 'Extension')
    .map(a => {
      const parts  = (a.application_number || '').split('/');
      const exIdx  = parts.findIndex(p => /^EX\d+$/i.test(p));
      const extTag = exIdx >= 0 ? parts[exIdx].toUpperCase() : null;   // "EX1", "EX2" …
      const extNum = extTag ? parseInt(extTag.slice(2)) : null;
      // Segments after the EX tag are: [approvalDDMMYY, validityDDMMYY?]
      const seg1   = exIdx >= 0 ? parts[exIdx + 1] : null;
      const seg2   = exIdx >= 0 ? parts[exIdx + 2] : null;
      // seg2 is the new completion (validity) date only when it is a distinct second segment
      // (older format has only one segment = the approval date itself, no separate validity)
      const newCompletion = (seg2 && seg2 !== seg1) ? parseDDMMYY(seg2) : null;
      return {
        applicationNo : a.application_number,
        date          : fmtDate(a.approval_date),
        extensionTag  : extTag,
        extensionNo   : extNum,
        newCompletionDate: fmtDate(newCompletion),
      };
    })
    .sort((a, b) => (a.extensionNo || 0) - (b.extensionNo || 0));

  // Alterations — plan revisions approved by RERA under Section 14/15
  // Application number format: .../A{n}{code}/{dateDDMMYY}
  const alterations = allAppsArr
    .filter(a => a.type === 'Alteration')
    .map(a => {
      const parts    = (a.application_number || '').split('/');
      // Alteration code is like A1C, A1M, A2C — letter A + digit(s) + optional letter
      const altCode  = parts.find(p => /^A\d+[A-Z]*$/i.test(p)) || null;
      const altNum   = altCode ? parseInt(altCode.slice(1)) : null;
      return {
        applicationNo : a.application_number,
        date          : fmtDate(a.approval_date),
        alterationCode: altCode ? altCode.toUpperCase() : null,
        alterationNo  : altNum,
      };
    })
    .sort((a, b) => (a.alterationNo || 0) - (b.alterationNo || 0));

  // Dates & status — BUG 1: form1 (progress-report API) is often empty; form1Full (byformone-id) has the dates.
  // QPR block progress has blk_dev_end_date = the original (pre-extension) dev end date per block.
  const startDate = form1?.projectStartDate
    || form1Full?.projectStartDate
    || qprForm1Full?.projectStartDate
    || qprBlockProgressArr[0]?.blk_dev_start_date
    || c.projectStartDate || c.startDate || c.projStartDate || null;

  // originalEndDate = pre-extension; use QPR block dev end (unaffected by extension approval)
  // then fall back through other sources. form1Full.projectEndDate may already reflect the extension.
  const qprBlockEndDate = qprBlockProgressArr[0]?.blk_dev_end_date || null;
  const endDate = form1?.projectEndDate
    || qprBlockEndDate
    || qprForm1Full?.projectEndDate
    || c.projectEndDate || c.projEndDate || c.endDate || null;

  // BUG 5: effective completion date = latest extension's new deadline
  // Fallback chain: extension newCompletionDate → form1Full.projectEndDate (already updated by RERA after extension) → endDate
  const lastExtWithNewDate      = [...extensions].reverse().find(e => e.newCompletionDate);
  const effectiveCompletionDate = lastExtWithNewDate?.newCompletionDate
    || form1Full?.projectEndDate
    || endDate;
  const progressReport = form1?.progressReport   ?? null;
  const now            = new Date();
  // Use effectiveCompletionDate (accounts for approved extensions) so projects with valid
  // extensions are not falsely marked lapsed against their original deadline.
  const completion     = effectiveCompletionDate ? new Date(effectiveCompletionDate) : null;
  let status = 'active';
  if (progressReport === 100 || (completion && completion < now && progressReport >= 90)) {
    status = 'completed';
  } else if (completion && completion < now && progressReport < 50) {
    status = 'lapsed';
  }

  // Dashboard block progress map (fallback source)
  const qprProgressMap = new Map(
    qprBlockProgressArr.map(b => [(b.blk_name || '').toUpperCase(), b])
  );

  // Form 1 block map — workDonePer (construction %) + commonWorkDone (common areas %)
  // Safe percent parser for RERA API progress fields.
  // RERA returns numeric strings ("75.5", "0", "100") OR the text "COMPLETED" for fully done items.
  // parseFloat("COMPLETED") = NaN which silently becomes 0, understating progress.
  // _parsePct maps "COMPLETED" → 100; returns null for any other non-numeric text so callers
  // can decide whether to treat it as 0 or exclude it from averages.
  const _parsePct = v => {
    if (v == null) return null;
    const n = parseFloat(v);
    if (!isNaN(n)) return n;
    if (/^completed$/i.test(String(v).trim())) return 100;
    return null;
  };

  // Use latestF1Date (= latestCertDate; ALL non-QPR types including Bank Withdrawal update Form 1).
  // If latestCertDate is more recent than latest QPR → use project's own form1Full
  // (c.formOneId, updated by RERA after every non-QPR approval); else QPR Form 1.
  const _blockProgressSrc = (latestF1Date > latestQprDate)
    ? (form1Full?.formOneAList || [])
    : (qprForm1Full?.formOneAList || []);
  const qprForm1BlockMap = new Map(
    _blockProgressSrc
      .map(b => [(b.blockName || '').toUpperCase().trim(), b])
  );

  // Blocks — primary source: Form 1 workDonePer + commonWorkDone (date-comparison winner)
  // Fallback: dashboard block_progress if Form 1 entry absent.
  const form2Blocks = form2?.formTwoAList || [];
  const blocks = form2Blocks.length > 0
    ? form2Blocks.map(b => {
        const key = (b.blockName || '').toUpperCase();
        const f1b = qprForm1BlockMap.get(key);   // date-comparison winner: alteration Form 1 or QPR Form 1
        const qb  = qprProgressMap.get(key);     // dashboard fallback
        const workDone   = f1b?.workDonePer   != null ? (_parsePct(f1b.workDonePer)    ?? 0) : (qb ? (_parsePct(qb.block_progress) ?? 0) : (_parsePct(b.workDonePer) ?? 0));
        const commonDone = f1b?.commonWorkDone != null ? _parsePct(f1b.commonWorkDone)        : null;
        return {
          name           : b.blockName,
          estimatedCost  : b.estimatedCost,
          costIncurred   : b.costInncurred,
          workDonePercent: workDone,
          commonWorkDone : commonDone,
          lastUpdated    : fmtDate(b.inncurredOn),
        };
      })
    // Fallback: formTwo has no blocks — use QPR block list directly
    : qprBlockProgressArr.map(b => {
        const key = (b.blk_name || '').toUpperCase().trim();
        const f1b = qprForm1BlockMap.get(key);
        return {
          name           : b.blk_name,
          estimatedCost  : null,
          costIncurred   : null,
          workDonePercent: f1b?.workDonePer != null ? (_parsePct(f1b.workDonePer) ?? 0) : (_parsePct(b.block_progress) ?? 0),
          commonWorkDone : f1b?.commonWorkDone != null ? _parsePct(f1b.commonWorkDone) : null,
          lastUpdated    : b.blk_dev_end_date || null,
        };
      });

  const totalEstimatedCost = form2?.formTwoB?.estimatedCost || null;
  const totalCostIncurred  = form2?.formTwoB?.costInncurred || null;
  // Always compute from block averages (1 decimal), never cache/hardcode.
  // Exclude null workDonePercent values (unknown text) — a block with no reported % should not
  // drag the average down to 0.
  const _blockPcts = blocks.map(b => b.workDonePercent).filter(v => v != null);
  const blockAvg = _blockPcts.length > 0
    ? parseFloat((_blockPcts.reduce((s, v) => s + v, 0) / _blockPcts.length).toFixed(1))
    : null;
  // Amenities — use date-comparison winner Form 1B (same certificate as construction/booking data)
  const _amenitiesSrc = (latestF1Date > latestQprDate) ? form1Full : qprForm1Full;
  const amenities = extractAmenities(_amenitiesSrc?.formOneB);

  // Common amenities % — uses the same date-comparison source as block construction percentages.
  // _blockProgressSrc = form1Full when latestF1Date > latestQprDate (alteration/extension/BWA more
  // recent than QPR), else qprForm1Full. This mirrors exactly how workDonePer is sourced per block.
  // Zero values are included (a genuine 0% means work hasn't started — still valid data).
  // Fallback: individual amenity workDone from formOneB if formOneAList has no commonWorkDone data.
  const commonAmenitiesPercent = (() => {
    // Primary: same source as block construction % (date-comparison winner)
    const vals = _blockProgressSrc
      .map(b => _parsePct(b.commonWorkDone ?? b.commonAmenitiesPercent ?? b.commonAmenPercent))
      .filter(v => v !== null);
    if (vals.length > 0) {
      const result = parseFloat((vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1));
      const src = latestF1Date > latestQprDate ? 'form1Full' : 'qprForm1Full';
      console.log(`[CommonArea] ${projectId} | src=${src} | vals=${JSON.stringify(vals)} → ${result}%`);
      return result;
    }
    // Fallback: individual amenity workDone from formOneB (include zeros — 0% is valid)
    const included = amenities.filter(a => a.included);
    if (included.length === 0) return null;
    const withVals = included.map(a => _parsePct(a.workDone)).filter(v => v !== null);
    if (withVals.length === 0) return null;
    const result = parseFloat((withVals.reduce((s, v) => s + v, 0) / withVals.length).toFixed(1));
    console.log(`[CommonArea] ${projectId} | src=formOneB_amenities | included=${included.length} vals=${JSON.stringify(withVals)} → ${result}%`);
    return result;
  })();

  // overallProgress = blockAvg (block construction average from QPR).
  // Matches what RERA website shows as block progress. commonAmenitiesPercent is returned
  // separately and shown as its own row in the construction card.
  const overallProgress = blockAvg ?? progressReport ?? null;

  // Banks — mask account number to match RERA portal display (last 4 digits visible)
  const banksData = unwrap(banksRaw, banksRaw);
  const bank = banksData ? {
    bankName      : banksData.bankName     || null,
    branchName    : banksData.branchName   || null,
    ifscCode      : banksData.ifscCode     || null,
    accountNumber : maskAccountNo(banksData.accountNumber),
    depositAmt    : parseFloat(banksData.depositAmt   || 0) || null,
    withdrawalAmt : parseFloat(banksData.withdrwalAmt || 0) || null,
    closingBal    : parseFloat(banksData.closingBal   || 0) || null,
  } : null;

  // Land owners
  // NOTE: The RERA public land-owner API (/project_reg/public/land-owner/project?projectId=)
  // has a server-side bug where it ignores the projectId param and always returns the same
  // record (prjRegId 25414). We validate the response by checking prjRegId matches projectId;
  // if it doesn't, the data is from a different project and we discard it.
  const landArr = unwrapArr(landRaw).filter(l => {
    const returnedPrjId = l.prjRegId || l.projectId;
    return !returnedPrjId || parseInt(returnedPrjId) === projectId;
  });
  const landOwners = landArr.map(l => ({
    name : `${l.firstName || ''} ${l.lastName || ''}`.trim(),
    pan  : l.pan   || null,
    type : l.promoterType || null,
  })).filter(l => l.name);

  // Previous / other projects
  const prevData    = unwrap(prevProjRaw, prevProjRaw);
  const gujreraProj = (prevData?.gujrera || []).map(p => ({
    name   : p.projectName,
    regNo  : p.regNo,
    status : p.status,
    cost   : p.cost,
  }));
  const prevList = prevData?.pervlist || [];

  // Promoter / engineer details
  const promoterDetData = unwrap(promoterDetRaw, promoterDetRaw);
  const engineers = (promoterDetData?.englist || []).map(e => ({
    name              : e.name,
    licenceNo         : e.licenceNO,
    projectsCompleted : e.noOfKeyProjectsCompleted,
    experience        : e.profExperience,
  }));

  // Unit summary — dashboard API returns data as an array, grab first element.
  // unitData.totunit is RERA's real-time total — updated whenever any application
  // (alteration, extension, registration) is approved. It is the same value RERA portal displays.
  // Form 3A numberOfUnits can be stale on the alteration cert (e.g. CAA14735: cert has 115,
  // totunit = 154 after alteration approved). totunit is primary; Form 3A as fallback.
  const unitRawUnwrapped = unwrap(unitRaw, unitRaw);
  const unitData = Array.isArray(unitRawUnwrapped) ? unitRawUnwrapped[0] : unitRawUnwrapped;
  // Unit count source priority:
  // 1. QPR Form 3A numberOfUnits — promoter declares post-alteration total in QPR submission.
  //    This is the only source confirmed to carry the correct count after unit-count alterations
  //    (CAA14735: QPR Form 3A = 154; alteration cert Form 3A = 115 stale; totunit = 115 stale).
  //    Only available when QPR cert differs from latest cert (alteration > QPR scenario).
  // 2. Dashboard totunit — real-time RERA total; used as primary when QPR Form 3A is unavailable.
  // 3. Latest cert Form 3A numberOfUnits — fallback when both above are missing/zero.
  const _qprF3aTotalUnits  = parseInt(qprForm3A?.numberOfUnits || qprForm3A?.totalUnits || 0) || null;
  const _dashTotalUnits    = parseInt(unitData?.totunit || 0) || null;
  const _f3aTotalUnits     = parseInt(form3A?.numberOfUnits || form3A?.totalUnits || 0) || null;
  const _totalUnits        = _qprF3aTotalUnits || _dashTotalUnits || _f3aTotalUnits || null;
  console.log(`[UnitCount] ${projectId} | qprF3a=${_qprF3aTotalUnits} totunit=${_dashTotalUnits} certF3a=${_f3aTotalUnits} → using=${_totalUnits}`);
  const unitSummary = unitData ? {
    totalUnits     : _totalUnits,
    maxCost        : parseFloat(unitData.maxcost)    || null,
    minCost        : parseFloat(unitData.mincost)    || null,
    maxCarpetArea  : parseFloat(unitData.maxcararea) || null,
    minCarpetArea  : parseFloat(unitData.mincararea) || null,
    totalCarpetArea: parseFloat(unitData.totcararea) || null,
  } : null;

  // Block booking chart — primary shape for block names/totals; per-block booked counts
  // come from latestBookingMap (alteration form-one when alteration > QPR, else QPR form-one).
  // Never use Math.max — alterations can reduce booked counts.
  // When perc=100, cap booked at total regardless of source.
  const blockChartArr = unwrapArr(blockChartRaw);
  const blockBooking = blockChartArr.map(b => {
    const rawBooked   = parseInt(b.booked)   || 0;
    const rawUnbooked = parseInt(b.unbooked) || 0;
    const total       = rawBooked + rawUnbooked;
    const pct         = _parsePct(b.perc)    ?? 0;  // "COMPLETED" → 100, not NaN → 0
    const key         = _normKey(b.bname);  // normalized: spaces around +/- stripped
    // Per-block booked: use Form 1 mapping (date-comparison winner) when available.
    // Form 1 is authoritative — do NOT apply pct>=100 cap when a Form 1 entry exists,
    // because the block chart perc and total can both be stale after an alteration
    // (MAA10110 D+E+F+G: block chart shows perc=100 at 100 units; alteration reduced to 87).
    // pct>=100 override only applies when there is no Form 1 entry for this block.
    // Booking lookup — three levels:
    // 1. Exact key match (fast path, works for most blocks)
    // 2. Parts-based match — handles block merges after alteration where block chart still
    //    has old name "D+E+F+G" but Form 1 now uses combined name "D+E+F+G+H+I+J".
    //    Rule: if all parts of the block chart bname appear in a map key's parts, use that entry.
    // 3. Fallback to block chart data (pct>=100 → total, else rawBooked)
    let booked;
    if (latestBookingMap.has(key)) {
      booked = latestBookingMap.get(key);                  // exact match
    } else {
      const bParts = key.split('+').filter(Boolean);
      let partialVal = null;
      for (const [k, v] of latestBookingMap) {
        const kParts = k.split('+').filter(Boolean);
        if (bParts.length > 0 && bParts.every(p => kParts.includes(p))) {
          partialVal = v;
          console.log(`[BookingMap] ${projectId} | PARTS MATCH: bname="${b.bname}"→"${k}" booked=${v}`);
          break;
        }
      }
      if (partialVal !== null) {
        booked = partialVal;
      } else {
        if (latestBookingMap.size > 0) {
          console.log(`[BookingMap] ${projectId} | KEY MISS: bname="${b.bname}" key="${key}" | mapKeys=${JSON.stringify([...latestBookingMap.keys()])}`);
        }
        booked = pct >= 100 ? total : rawBooked;           // no Form 1 entry — use block chart
      }
    }
    const unbooked   = Math.max(0, total - booked);       // prevent negative if Form 1 > stale total
    // Always compute booking % from actual counts; never use pre-computed API percentage
    const bookingPct = total > 0 ? parseFloat((booked / total * 100).toFixed(1)) : null;
    return { blockName: b.bname, booked, unbooked, bookingPct };
  });
  const blockBookedTotal   = blockBooking.reduce((s, b) => s + b.booked,   0);
  const blockUnbookedTotal = blockBooking.reduce((s, b) => s + b.unbooked, 0);
  const blockTotalUnits    = blockBookedTotal + blockUnbookedTotal;

  // QPR history — only genuine quarterly return entries (Q-1, Q-2, Q-E, X-1, X-2, etc.)
  // BWA = plan amendments (kept separately); Registration/Extension/Alteration entries excluded.
  const qprArr     = unwrapArr(qprHistoryRaw);
  const bwaEntries = qprArr
    .filter(q => (q.quarterName || '').toString().toUpperCase().startsWith('BWA'))
    .map(q => ({ quarterName: q.quarterName, startDate: fmtDate(q.startDate), endDate: fmtDate(q.endDate) }));

  // Genuine QPR entry = quarterName starts with Q- / Q / X- / X followed by digits or E
  const isQPREntry = name => /^(Q-?\d+|Q-?E|X-?\d+)/i.test((name || '').toString().trim());

  const qprHistory = qprArr
    .filter(q => isQPREntry(q.quarterName))
    .map(q => {
      // RERA `status` field is authoritative:
      //   SUBMITTED  = filed (on time or late) — RERA website shows as filed, NOT defaulted
      //   EXEMPT     = exempted quarter (COVID, future period) — NOT counted in required/filed/defaulted
      //   anything else + isDefault=YES = genuinely defaulted
      // NOTE: RERA sometimes leaves isDefault='Y' even after a promoter files (bug in RERA backend).
      // The `status` field is the reliable signal — if status=SUBMITTED, it was filed. Period.
      const statusRaw   = (q.status || '').toString().toUpperCase().trim();
      const isExempt    = statusRaw === 'EXEMPT';
      const isSubmitted = statusRaw === 'SUBMITTED';
      // isDefault=YES on EXEMPT or SUBMITTED quarters is a RERA data inconsistency — ignore it
      const isDefault = !isExempt && !isSubmitted && (
        q.isDefault === 'Y' || q.isDefault === 'y' ||
        q.isDefault === 'YES' || q.isDefault === 'yes' ||
        q.isDefault === true || q.isDefault === 1
      );
      return {
        quarterName : q.quarterName,
        isDefault,
        isExempt,
        status      : statusRaw || null,
        startDate   : fmtDate(q.startDate),
        endDate     : fmtDate(q.endDate),
        submittedOn : q.submittedOnStr || null,
      };
    });

  // Totals — EXEMPT quarters are excluded from all counts (matches RERA certificate details page)
  const compRequired  = qprHistory.filter(q => !q.isExempt).length;
  const compDefaulted = qprHistory.filter(q => q.isDefault).length;
  const compComplied  = compRequired - compDefaulted;
  const qprSummaryTotals = {
    required : compRequired,
    complied : compComplied,
    defaulted: compDefaulted,
    pending  : Math.max(0, compRequired - compComplied - compDefaulted),
  };
  const qprDefaultedCount = qprSummaryTotals.defaulted;

  // Q-E (Quarter-End) Return — when filed it signals project completion.
  // After the FY in which Q-E was filed, Form 5 annual returns are no longer required.
  //
  // RERA uses inconsistent naming across APIs for Q-E: "Q-E", "QE", "Q E",
  // "Quarter End", "Quarter-End Return", "QUARTER END QPR", etc.
  // _isQeQuarterName() normalises all variants so no Q-E filing is missed.
  const _isQeQuarterName = name => {
    const u = (name || '').toString().toUpperCase().trim();
    const noSep = u.replace(/[-\s]/g, '');   // strip hyphens + spaces for normalised compare
    if (noSep === 'QE') return true;                            // "Q-E", "QE", "Q E" exact
    if (noSep === 'FORM4') return true;                         // RERA projectAllApplications uses "Form4" / "Form 4"
    if (u.startsWith('Q-E')) return true;                       // "Q-E Return", "Q-E QPR", …
    if (/^Q\s+E\b/i.test(u)) return true;                       // "Q  E" multi-space variants
    if (u.includes('QUARTER') && u.includes('END')) return true; // "Quarter End", "Quarter-End Return"
    return false;
  };

  // Primary: look for Q-E in projectAllApplications
  const qeAppEntry = extractArray(projectAppsRaw).find(a => _isQeQuarterName(a.type));

  // Scan raw qprArr (getprojectqtrs) directly — RERA includes Q-E here alongside Q-1, Q-2, BWA, etc.
  // Search qprArr (not filtered qprHistory) so we catch Q-E regardless of how isQPREntry behaves.
  const qeRawEntry = qprArr.find(q => _isQeQuarterName(q.quarterName));

  // Also check filtered qprHistory (used for FY cutoff when endDate is needed from mapped entry)
  const qeHistEntry = qprHistory.find(q => _isQeQuarterName(q.quarterName));


  // Priority: direct Q-E return API (most reliable) → projectAllApplications → qprHistory → qprArr raw
  const qeDate = _qeReturnFiled
    ? (fmtDate(qeReturnRaw.submitted_on) || 'Submitted')
    : qeAppEntry
      ? fmtDate(qeAppEntry.approval_date || null)
      : qeHistEntry
        ? (qeHistEntry.submittedOn || qeHistEntry.endDate || 'Submitted')
        : qeRawEntry
          ? (fmtDate(qeRawEntry.endDate) || fmtDate(qeRawEntry.startDate) || 'Submitted')
          : null;
  // For Form 5 FY cutoff: use QPR history endDate (actual quarter end date, always within correct FY)
  // Approval date can be in the next FY (e.g. Q-E for Mar 2024 approved in April 2024 → wrong FY).
  const _qeFYDateSrc = qeHistEntry?.endDate || qeRawEntry?.endDate || qeAppEntry?.approval_date || null;
  const qeFYEndYear = _qeFYDateSrc
    ? getFYEndYear(fmtDate(_qeFYDateSrc) || _qeFYDateSrc.toString())
    : null;
  const qeApplicationNo = qeAppEntry?.application_number || null;
  const qePeriod = qeRawEntry ? {
    from : fmtDate(qeRawEntry.startDate) || null,
    to   : fmtDate(qeRawEntry.endDate)   || null,
  } : null;
  const qeReraAction = qeRawEntry ? {
    enquiryStatus : qeRawEntry.enquiryStatus || null,
    authRemarks   : qeRawEntry.authRemarks   || null,
    isOpened      : qeRawEntry.isAuthOpened  || null,
    openedOn      : fmtDate(qeRawEntry.authOpenedOn) || null,
    orderFileUid  : qeRawEntry.orderFileUid  || null,
    penaltyAmt    : qeRawEntry.penaltyAmt    || null,
  } : null;

  // qprHistoryDisplay = qprHistory with Q-E appended at the end when Q-E is filed.
  // getprojectqtrs API sometimes omits Q-E from the QPR list even when it exists in
  // projectAllApplications. We detect this and add a synthetic display entry.
  // The entry has isQeEntry:true so the frontend can style it distinctly.
  // NOTE: Stats (required/complied/defaulted) were already computed from qprHistory BEFORE
  // this append, so counts remain correct.
  const _qeAlreadyInHistory = qprHistory.some(q => _isQeQuarterName(q.quarterName));
  const qprHistoryDisplay = (!_qeAlreadyInHistory && (qeAppEntry || qeRawEntry || _qeReturnFiled))
    ? [...qprHistory, {
        quarterName : 'Q-E',
        isDefault   : false,
        isExempt    : false,
        isQeEntry   : true,
        status      : 'SUBMITTED',
        startDate   : qePeriod?.from || null,
        endDate     : qePeriod?.to   || null,
        submittedOn : qeDate || null,
      }]
    : qprHistory;

  // Annual compliance — BUG 3 FIX: flag overdue only if due date has passed
  // Additional fix: filter out stub entries with no status (API may return empty FY records such
  // as FY 2020-21 for projects that were not yet registered in that year).
  // Q-E fix: exclude FYs after the Q-E filing year — Form 5 not required post Q-E.
  // Registration fix: exclude FYs before the project's RERA approval year — Form 5 obligation
  // only starts from the FY in which the project was registered with RERA.
  const _regApprovalDate = (c.approvedDate && c.approvedDate !== 'Date Not Found')
    ? _pd2(c.approvedDate) : (_pd2(startDate) || null);
  const regFYEndYear = _regApprovalDate ? getFYEndYear(_regApprovalDate.toISOString()) : null;
  const annualArr = unwrapArr(annualRaw)
    .filter(a => !!(a.projectFinancialYearStatus || '').trim())
    .filter(a => {
      // Drop FYs that precede the project's registration FY
      if (regFYEndYear) {
        const parts = (a.projectFinancialYear || '').toString().split('-');
        const twoDigit = parseInt(parts[1]);
        if (!isNaN(twoDigit)) {
          const fyEndYear = twoDigit < 50 ? 2000 + twoDigit : 1900 + twoDigit;
          if (fyEndYear < regFYEndYear) return false;
        }
      }
      return true;
    })
    .filter(a => {
      if (!qeFYEndYear) return true;
      // FY format "2021-22" — two-digit end offset
      const parts = (a.projectFinancialYear || '').toString().split('-');
      const twoDigit = parseInt(parts[1]);
      if (isNaN(twoDigit)) return true;
      const fyEndYear = twoDigit < 50 ? 2000 + twoDigit : 1900 + twoDigit;
      return fyEndYear <= qeFYEndYear;
    });
  const todayForDue = new Date();
  const annualCompliance = annualArr.map(a => {
    const fy       = a.projectFinancialYear;
    const status   = a.projectFinancialYearStatus;
    const submitted = status === 'SUBMITTED';
    const dueDate   = getFilingDueDate(fy);
    const overdue   = !submitted && dueDate ? todayForDue > dueDate : false;
    return {
      year     : fy,
      status,
      submitted,
      overdue,
      dueDate  : dueDate ? dueDate.toISOString().split('T')[0] : null,
    };
  });

  // Form3A assembly
  // form3A       = latest cert Form 3A (formThreeId = loanDetailsFormThreeId) — fund flow, booking
  // qprForm3A    = QPR cert Form 3A (qprFormThreeId, only when ≠ formThreeId) — unit counts
  // _totalUnits  = qprF3a → totunit → certF3a (see priority comment above)
  const _fundFlowSrc = form3A;
  const f3a = (form3A || qprForm3A || unitData) ? {
    totalUnits     : _totalUnits,
    bookedUnits    : form3A?.bookedUnit      || null,
    unbookedUnits  : form3A?.unBookedUnit    || null,
    totalCarpetArea: form3A?.totalCarpetArea || null,
    bookedCarpetArea: form3A?.bookedCarpetArea || null,
    receivedAmount : _fundFlowSrc?.receivedAmountTotal || null,
    balanceAmount  : _fundFlowSrc?.balanceAmountTotal  || null,
  } : null;

  // Form3C (project financials)
  const f3c = form3C ? {
    totalEstimatedCost: form3C.totalEstimatedCostofTheRealEstateProject || null,
    totalCostIncurred : form3C.totalCostIncurredandPaid || null,
    loanAmountTaken   : form3C.loanAmountTaken  || 0,
    loanBalance       : form3C.loanBalance      || 0,
    landCost          : form3C.subTotalofLandCostA   || null,
    developmentCost   : form3C.subTotofDevelopCostA  || null,
  } : null;

  // Block stages (BU/OC indicators) — use date-comparison winner (same source as construction % and booking)
  const _blockStagesSrc = (latestF1Date > latestQprDate) ? form1Full : qprForm1Full;
  const blockStages = Array.isArray(_blockStagesSrc?.formOneAList) ? _blockStagesSrc.formOneAList : [];

  // Developer Track Record — from promoter group entity API
  const promoterGroup = promoterGroupRaw ? {
    groupName        : promoterGroupRaw.entities_developerGroupName || null,
    experienceInState: promoterGroupRaw.entities_experienceInState  || null,
    projectsCompleted: promoterGroupRaw.entities_noOfProjectsCompleted || null,
    ongoingProjects  : promoterGroupRaw.entities_ongoingProjects    || null,
    areaConstructed  : promoterGroupRaw.entities_areaConstructed    || null,
  } : null;

  // Possession Readiness Checklist — per-block BU/Society data from NOC endpoint
  // API returns { blocksNocDocList: [...], formOneNOCDoc: {...} } — not the standard { data: [] } shape
  const possessionArr = Array.isArray(possessionRaw?.blocksNocDocList)
    ? possessionRaw.blocksNocDocList
    : unwrapArr(possessionRaw);

  // Diagnostic: log raw possession data to find BU PDF UID field names
  if (possessionArr.length > 0) {
    console.log(`[Possession] ${projectId} blocksNocDocList[0] keys:`, Object.keys(possessionArr[0]).join(', '));
    console.log(`[Possession] ${projectId} blocksNocDocList[0] raw:`, JSON.stringify(possessionArr[0]));
  }
  const _formOneNOCDoc = possessionRaw?.formOneNOCDoc || {};
  console.log(`[Possession] ${projectId} formOneNOCDoc keys:`, Object.keys(_formOneNOCDoc).join(', ') || '(none)');
  console.log(`[Possession] ${projectId} formOneNOCDoc raw:`, JSON.stringify(_formOneNOCDoc));

  const _pdBuUid = _pd.buCertificateUId || _pd.buPdfDocUId || _pd.buAppDocUId || null;

  // BU date/number/docUId per block.
  // BU is ALWAYS entered in Form 1 at Q-E filing time — Q-E Form 1 is the ONLY reliable source.
  // QPR Form 1 and Registration Form 1 will not have BU data (BU is not submitted at those stages).
  // Source priority: Q-E Form 1 (qeForm1Full, validated by projectId) → QPR Form 1 → Registration Form 1.
  const _f1AList = (qeForm1Full?.formOneAList?.length > 0)
    ? qeForm1Full.formOneAList
    : (qprForm1Full?.formOneAList?.length > 0
        ? qprForm1Full.formOneAList
        : (form1Full?.formOneAList || []));
  const _f1BuMap = new Map();
  for (const b of _f1AList) {
    const key = _normKey(b.blockName || '');
    if (key) {
      _f1BuMap.set(key, {
        buDate  : fmtDate(b.buDate || b.buildingUseDate || b.buPermissionDate) || null,
        buNumber: b.buNumber || b.buildingUseNo || b.buPermissionNo || null,
        buDocUId: b.buDocUId || b.buCertificateUId || b.buPdfDocUId || b.buCertDocUId || null,
      });
    }
  }
  // Diagnostic: log BU fields from the source list
  if (_f1AList.length > 0) {
    const _b0    = _f1AList[0];
    const _buSrc = qeForm1Full?.formOneAList?.length > 0 ? 'qeForm1Full'
                 : qprForm1Full?.formOneAList?.length > 0 ? 'qprForm1Full'
                 : 'form1Full';
    console.log(`[BUCheck] ${projectId} src=${_buSrc} block[0]=${_b0.blockName} buDate=${_b0.buDate} buNumber=${_b0.buNumber} buDocUId=${_b0.buDocUId} buCheck=${_b0.buCheck} buPermissionDate=${_b0.buPermissionDate} buPermissionNo=${_b0.buPermissionNo}`);
  }

  // Build possession checklist: prefer POSSESSION_API block data, fall back to Form 1 blocks
  const _checklistSource = possessionArr.length > 0 ? possessionArr : _f1AList;
  // Q-E Form 4 UID — used as BU source fallback when structured BU data is unavailable.
  // RERA requires BU as a mandatory part of Q-E filing. If Form 4 was accepted (status=SUBMITTED),
  // BU was declared in it. We surface this so the report shows "See Form 4" instead of "Not Submitted".
  const _qeForm4Uid = _qeReturnFiled ? (qeReturnRaw?.qt_form_four_a_docuid || null) : null;
  const possessionChecklist = _checklistSource.map(b => {
    const blkKey = _normKey(b.blkName || b.blockName || '');
    const f1Bu   = _f1BuMap.get(blkKey) || {};
    const buDate   = fmtDate(b.buDate) || f1Bu.buDate || null;
    const buNumber = b.buNumber || f1Bu.buNumber || null;
    const buPdfUId = b.buPdfDocUId || f1Bu.buDocUId || _pdBuUid || null;
    const hasBU    = b.buCheck === 'YES' || !!(buNumber || buDate);
    // buFromForm4: true when BU data is not in structured fields but Q-E Form 4 was filed.
    // RERA requires BU submission as part of Q-E — Form 4 accepted = BU was declared.
    const buFromForm4 = !hasBU && !!_qeForm4Uid;
    return {
      blockName   : b.blkName || b.blockName || null,
      buCheck     : b.buCheck || null,
      buDate,
      buNumber,
      buPdfDocUId : buPdfUId,
      buAppNumber : b.buAppNumber || null,
      buAppDate   : fmtDate(b.buAppDate) || null,
      hasBU,
      buFromForm4,
      // When buFromForm4, the Form 4 PDF is the source to verify BU — pass its UID for download link
      buForm4DocUId: buFromForm4 ? _qeForm4Uid : null,
    };
  });

  /* ── Build final response object ── */
  const result = {
    // Identifiers
    projectId      : projectId,
    projectName,
    registrationNo : c.projRegNo   || c.projectAckNo || 'N/A',
    ackNo          : c.projectAckNo || null,

    // Status
    status,
    projectType    : c.projectType  || mapData?.projectType || 'N/A',
    processType    : mapData?.processType || 'N/A',

    // Promoter
    promoterName   : (c.promoterName  || 'N/A').trim(),
    promoterType   : (c.promoterType  || 'N/A').trim(),
    promoterEmail  : c.promoterEmailId || null,
    promoterMobile : c.promoterMobileNo || null,

    // Location
    district       : mapData?.districtName || 'N/A',

    // Dates — BUG 1: null → "Not Available"; use effectiveCompletionDate for extensions
    applicationDate        : c.appSubmissionDate ? fmtDate(c.appSubmissionDate) : 'Not Available',
    approvedDate           : (c.approvedDate && c.approvedDate !== 'Date Not Found') ? (fmtDate(c.approvedDate) || c.approvedDate) : 'Pending',
    projectStartDate       : startDate ? fmtDate(startDate) : 'Not Available',
    completionDate         : effectiveCompletionDate ? fmtDate(effectiveCompletionDate) : 'Not Available',
    originalCompletionDate : endDate ? fmtDate(endDate) : null,

    // Construction
    architectName    : form1?.architectName   || null,
    coaRegNo         : form1?.coaRegNo        || null,
    physicalVisitDate: form1?.physicalVisitDate ? fmtDate(form1.physicalVisitDate) : null,
    overallProgress,

    // Blocks
    blocks,
    totalBlocks    : blocks.length,
    blocksComplete : blocks.filter(b => b.workDonePercent >= 99).length,

    // Financials (from formtwo or form3c)
    totalEstimatedCost : f3c?.totalEstimatedCost || totalEstimatedCost,
    totalCostIncurred  : f3c?.totalCostIncurred  || totalCostIncurred,

    // Feature 1: Complaints & Disputes
    complaints,
    complaintCount : complaints.length,

    // Feature 2: Unit-wise Summary
    unitSummary,
    blockBooking,
    form3A : f3a,
    // BUG 5 FIX: booking counts from block chart, adjusted for alterations.
    // Block-chart API is not updated when a project alteration changes units (add OR reduce),
    // so its total can be stale. Form3A (using the date-comparison-selected formThreeId —
    // whichever of QPR or latest alteration certificate is newer) is authoritative.
    // Use Form3A total when present; fall back to block chart only if missing.
    unitBookingFromBlocks: (() => {
      const f3aTotal   = parseInt(f3a?.totalUnits  || 0) || 0;
      const f3aBooked  = parseInt(f3a?.bookedUnits || 0) || 0;
      const adjTotal   = f3aTotal > 0 ? f3aTotal : blockTotalUnits;
      if (adjTotal <= 0) return null;
      // Per-block Form 1 booking sum is most accurate when available (parts-based map).
      // Form3A booked can be stale after alterations (e.g. Extension carries pre-alteration count).
      // When latestBookingMap has per-block data, blockBookedTotal is authoritative.
      const adjBooked = latestBookingMap.size > 0
        ? blockBookedTotal
        : (f3aBooked > 0 ? f3aBooked : blockBookedTotal);
      const adjUnbooked = adjTotal - adjBooked;
      return { booked: adjBooked, unbooked: adjUnbooked, total: adjTotal };
    })(),

    // Feature 3: Compliance Health
    qprHistory: qprHistoryDisplay,  // includes synthetic Q-E entry at end when Q-E is filed
    qprSummaryTotals,    // BUG 3: Required/Complied/Defaulted/Pending from API or computed
    bwaEntries,          // BUG 3: BWA plan amendments (separate from QPRs)
    qprDefaultedCount,
    annualCompliance,
    hasQe: !!(qeAppEntry || qeHistEntry || qeRawEntry || _qeReturnFiled), // true if Q-E is filed by any signal
    qeDate,              // Date Q-E return was filed (null if not filed yet)
    qeApplicationNo,     // Application number from projectAllApplications for Q-E
    qePeriod,            // { from, to } — quarter period for Q-E return
    qeReraAction,        // RERA action on Q-E: enquiryStatus, authRemarks, isOpened, openedOn, orderFileUid, penaltyAmt
    // Q-E documents from direct API — Form 4, Affidavit, Revenue Record, Society Reg Certificate
    qeDocuments: _qeReturnFiled ? [
      qeReturnRaw.qt_form_four_a_docuid && { label: 'Q-E Form 4 (Quarter-End Return)',    uid: qeReturnRaw.qt_form_four_a_docuid,  filename: 'QE_Form4.pdf' },
      qeReturnRaw.qt_addidavit_docuid   && { label: 'Q-E Affidavit',                      uid: qeReturnRaw.qt_addidavit_docuid,    filename: 'QE_Affidavit.pdf' },
      qeReturnRaw.qt_revenue_docuid     && { label: 'Revenue Record (Property Card)',      uid: qeReturnRaw.qt_revenue_docuid,      filename: 'QE_Revenue_Record.pdf' },
      qeReturnRaw.qt_society_reg_docuid && { label: 'Society Registration Certificate',   uid: qeReturnRaw.qt_society_reg_docuid,  filename: 'Society_Registration_Certificate.pdf' },
      qeReturnRaw.qt_notarized_docuid   && { label: 'Q-E Notarized Document',             uid: qeReturnRaw.qt_notarized_docuid,    filename: 'QE_Notarized.pdf' },
    ].filter(Boolean) : [],
    qeSocietyRegDate: _qeReturnFiled ? (fmtDate(qeReturnRaw.society_reg_date) || null) : null,
    blockBuDates: possessionChecklist.filter(b => b.buDate).map(b => ({ blockName: b.blockName, buDate: b.buDate })),

    // Feature 4: BU Permission / OC per block
    blockStages,

    // Feature 5: Promoter Details (engineers)
    engineers,

    // Developer Track Record (promoter group entity)
    promoterGroup,

    // Possession Readiness Checklist (BU/Society per block)
    possessionChecklist,

    // Application flags
    hasForm4,
    hasRevenueRecord,
    hasAffidavit,
    hasSocietyFromApps,
    hasBuildingPermissionDoc,

    // Project Timeline — extensions and plan alterations from projectAllApplications
    extensions,
    alterations,

    // Feature 6: Collection Bank
    bank,

    // Feature 7: Common Amenities
    amenities,
    commonAmenitiesPercent,

    // Feature 8: Project Land Details
    landOwners,

    // Feature 9: Other Projects by developer
    otherProjects: { gujrera: gujreraProj, previous: prevList },

    // Financial deep-dive (form3c)
    form3C : f3c,

    // Loan outstanding — sum of loanBalance from per-lender Form 3 loan details records.
    // loanBalance per record = loanDisbursalReceived − loanRepaid (actual outstanding, not sanctioned).
    // Source: QPR's formThreeId when QPR exists (Form 8 CA Certificate), else project's own (CA Certificate).
    loanOutstanding      : loanOutstanding > 0 ? loanOutstanding : null,
    loanOutstandingSource: loanOutstandingSource,

    // Certificate
    certificateId  : c.certificateId   || null,
    certificateUid : c.certificateUid  || null,

    // Links
    reraUrl   : `${RERA_BASE}/?/#/pp?id=${Buffer.from(projectId.toString()).toString('base64')}`,
    fetchedAt : new Date().toISOString(),
  };

  // ── Background data validation ─────────────────────────────────────────────────────────────────
  // Compare key assembled values against raw API sources to catch staleness or selection bugs.
  // All checks log with [DataCheck] prefix — discrepancies > 5% warrant investigation.
  (() => {
    const chk = (label, assembled, raw, threshold = 0.05) => {
      if (assembled == null || raw == null) return;
      const a = parseFloat(assembled), r = parseFloat(raw);
      if (isNaN(a) || isNaN(r) || r === 0) return;
      const diff = Math.abs(a - r) / Math.abs(r);
      if (diff > threshold)
        console.warn(`[DataCheck] ${projectId} | ${label}: assembled=${a} raw=${r} diff=${(diff*100).toFixed(1)}%`);
    };
    // Unit totals summary
    const usedTotal = parseInt(_totalUnits || 0) || 0;
    // Booked units: Form 3A vs block chart sum
    const f3aBooked = parseInt(form3A?.bookedUnit || 0) || 0;
    chk('bookedUnits(F3A vs blockChart)', f3aBooked, blockBookedTotal);
    // Overall construction %: assembled blocks vs QPR block progress dashboard
    const assembledPct = result.overallProgress;
    const dashPct = qprBlockProgressArr.length > 0
      ? parseFloat((qprBlockProgressArr.reduce((s, b) => s + parseFloat(b.block_progress || 0), 0) / qprBlockProgressArr.length).toFixed(1))
      : null;
    chk('overallProgress(assembled vs dashAvg)', assembledPct, dashPct, 0.10);
    // Certificate selection summary
    console.log(`[DataCheck] ${projectId} | certSrc="${loanOutstandingSource}" f3Id=${formThreeId} qprF3Id=${qprFormThreeId} | units=${usedTotal}(qprF3a=${_qprF3aTotalUnits} totunit=${_dashTotalUnits} certF3a=${_f3aTotalUnits}) booked=${f3aBooked} progress=${assembledPct}%`);
  })();
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  // Risk score (computed after result is assembled)
  result.riskScore      = computeRisk(result);
  result.salesAlignment = computeSalesAlignment(result);
  result.insights       = getProjectInsights(result);

  // Timeline / Delay Detector (uses raw dates before fmtDate formatting)
  result.timelineIndicator = calculateTimelineIndicator({
    startDate              : startDate,
    endDate                : endDate,
    effectiveCompletionDate: effectiveCompletionDate,
    overallProgress        : result.overallProgress,
    extensions             : result.extensions,
    status                 : result.status,
  });

  result.servedFrom = 'live';

  // BUG-3/4 FIX: Don't cache if too many Stage 2 fetches failed (rate-limiting / partial data).
  // Count Stage 2 rejections; skip cache write if more than 3 of 18 sub-requests failed.
  const stage2Results = [
    mapResult, form1Result, form2Result, form1FullResult, qprFormIdsResult,
    banksResult, landResult, prevProjectsResult, promoterDetResult,
    unitDetailsResult, blockChartResult, qprHistoryResult, annualCompResult,
    projectAppsResult, promoterGroupResult, possessionResult,
    projectDocResult, bankWithdrawalResult, qeReturnResult,
  ];
  const stage2Failures = stage2Results.filter(r => r.status === 'rejected').length;
  if (stage2Failures <= 3) {
    cacheWrite(projectId, result);
  } else {
    console.warn(`[Cache] Skipping cache for project ${projectId} — ${stage2Failures}/19 Stage 2 fetches failed (possible rate-limiting)`);
  }

  return result;
}

/* ─────────────────────────────────────────────────────────────────
   Find project by RERA registration number
   ───────────────────────────────────────────────────────────────── */
async function findProjectByRegNo(regNo) {
  const BATCH = 50;
  const MAX   = Math.min(searchIndex.length > 0 ? Math.max(...searchIndex.map(p => p.id)) + 1 : 10000, INDEX_MAX_ID);

  for (let start = 1; start <= MAX; start += BATCH) {
    const ids     = Array.from({ length: BATCH }, (_, i) => start + i).filter(id => id <= MAX);
    const results = await Promise.allSettled(
      ids.map(id => axiosGet(`${PROJECT_API}/${id}`, 8000))
    );
    for (let i = 0; i < results.length; i++) {
      if (results[i].status !== 'fulfilled') continue;
      const d = results[i].value?.data;
      if (!d) continue;
      const r = (d.projRegNo || d.projectAckNo || '').toUpperCase();
      if (r === regNo || r.includes(regNo) || regNo.includes(r.slice(-10))) {
        return { id: ids[i], data: d };
      }
    }
  }
  return null;
}

/* ─────────────────────────────────────────────────────────────────
   Utility
   ───────────────────────────────────────────────────────────────── */
const _MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function fmtDate(raw) {
  if (!raw) return null;
  let d = new Date(raw);
  // Handle DD-MM-YYYY or DD/MM/YYYY returned by RERA API (not valid ISO, so Date() fails)
  if (isNaN(d.getTime())) {
    const m = raw.toString().match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
    if (m) d = new Date(`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`);
  }
  if (isNaN(d.getTime())) return raw;
  return `${String(d.getDate()).padStart(2,'0')} ${_MONTHS[d.getMonth()]} ${d.getFullYear()}`;
}
/* Null-safe wrapper — returns '' instead of null for missing dates */
function formatRERADate(raw) { return fmtDate(raw) || ''; }

/* Mask bank account number — show only last 4 digits, rest replaced with ×
   Matches RERA portal masking: e.g. 57500000916460 → ××××××××××6460        */
function maskAccountNo(raw) {
  if (!raw) return null;
  const s = raw.toString().trim();
  if (s.length <= 4) return s;
  return '×'.repeat(s.length - 4) + s.slice(-4);
}

function fmtCrore(n) {
  const num = parseFloat(n) || 0;
  const cr  = num / 10000000;
  if (cr >= 1) return cr.toFixed(2) + ' Cr';
  const lac = num / 100000;
  return lac.toFixed(2) + ' L';
}

/* ── BUG 3 FIX: Annual filing due date utilities ──
   FY format: "2025-26"  →  end year = 2026  →  due = 31 Oct 2026             */
// Returns the FY end-year (e.g. 2025 for FY 2024-25) from a date string.
// India FY: Apr 1 – Mar 31.  Jan 2025 → still in FY ending Mar 2025 → 2025.
function getFYEndYear(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  const month = d.getMonth(); // 0-indexed: Jan=0, Mar=2, Apr=3
  return month < 3 ? d.getFullYear() : d.getFullYear() + 1;
}

// Parse a 6-digit DDMMYY string from a RERA application number segment into ISO date.
function parseDDMMYY(s) {
  if (!s || s.length !== 6 || !/^\d{6}$/.test(s)) return null;
  const dd = parseInt(s.slice(0, 2), 10);
  const mm = parseInt(s.slice(2, 4), 10) - 1;
  const yy = parseInt(s.slice(4, 6), 10);
  const year = yy < 50 ? 2000 + yy : 1900 + yy;
  const d = new Date(Date.UTC(year, mm, dd));
  return isNaN(d) ? null : d.toISOString().split('T')[0];
}

function getFilingDueDate(fy) {
  if (!fy) return null;
  const parts = fy.split('-');
  if (parts.length < 2) return null;
  const startYear = parseInt(parts[0]);
  if (isNaN(startYear)) return null;
  const endYear = startYear + 1;          // "2025-26" → 2026
  return new Date(Date.UTC(endYear, 9, 31)); // UTC to avoid timezone shift in toISOString()
}

function isFilingOverdue(fy, status, today) {
  if (status === 'SUBMITTED') return false;
  const due = getFilingDueDate(fy);
  if (!due) return false;
  return (today || new Date()) > due;
}

/* ─────────────────────────────────────────────────────────────────
   Daily Project Check — runs at 22:00 IST every night
   Finds projects registered today, caches their full data,
   triggers legal analysis, and logs a summary report.
   ───────────────────────────────────────────────────────────────── */

const DAILY_REPORT_DIR = path.join(CACHE_DIR, 'daily-reports');
if (!fs.existsSync(DAILY_REPORT_DIR)) fs.mkdirSync(DAILY_REPORT_DIR, { recursive: true });

/**
 * Parse DD-MM-YYYY date string into a Date object.
 */
function parseDDMMYYYY(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{2})-(\d{2})-(\d{4})$/);
  return m ? new Date(m[3], m[2]-1, m[1]) : null;
}

/**
 * Discover projects registered today by scanning the top N high-ID
 * projects from the index and checking their approvedDate.
 */
async function findTodaysProjects() {
  const todayStr = new Date().toLocaleDateString('en-GB', {
    day:'2-digit', month:'2-digit', year:'numeric'
  }).split('/').join('-'); // → DD-MM-YYYY

  console.log(`[DailyCheck] Scanning for projects approved on ${todayStr}…`);

  // Use the in-memory search index (already loaded at startup)
  if (!searchIndex || searchIndex.length === 0) {
    console.warn('[DailyCheck] Search index not loaded — skipping');
    return [];
  }

  const allIds  = searchIndex.map(p => p.id).filter(Boolean);
  // Focus on highest IDs: recent projects are most likely to be new today
  const topIds  = allIds.sort((a,b) => b-a).slice(0, 3000);

  const CONC    = 25;
  const found   = [];
  for (let i = 0; i < topIds.length; i += CONC) {
    const chunk = topIds.slice(i, i+CONC);
    const results = await Promise.all(chunk.map(async id => {
      try {
        const raw = await axiosGet(`${RERA_BASE}/project_reg/public/alldatabyprojectid/${id}`, 5000);
        const c   = Array.isArray(raw?.data) ? raw.data[0] : (raw?.data || raw);
        if (!c?.approvedDate) return null;
        const d = parseDDMMYYYY(c.approvedDate);
        const today = new Date();
        if (!d || d.getDate() !== today.getDate() || d.getMonth() !== today.getMonth() || d.getFullYear() !== today.getFullYear()) return null;
        return { id, name: (c.projectName||'').trim(), approvedDate: c.approvedDate, district: c.districtName||'' };
      } catch { return null; }
    }));
    found.push(...results.filter(Boolean));
    await new Promise(r => setTimeout(r, 40));
  }
  return found;
}

/**
 * Validate a single project fetched through our proxy.
 * Returns { id, name, issues[] } where each issue is { section, severity, msg }.
 */
async function validateProjectData(id, name) {
  const issues = [];
  const fail   = (s, m) => issues.push({ section: s, severity: 'FAIL', msg: m });
  const warn_  = (s, m) => issues.push({ section: s, severity: 'WARN', msg: m });

  let proxy;
  try {
    // Force fresh data (bypass cache) for newly registered projects
    const cached = cacheRead(id);
    if (cached) cacheDelete(id);  // clear stale cache so we get live data
    proxy = await fetchFullProject(id);
    if (!proxy) { fail('fetch', 'fetchFullProject returned null'); return { id, name, issues }; }
  } catch(e) { fail('fetch', e.message); return { id, name, issues }; }

  // Basic fields
  if (!proxy.projectName?.trim()) fail('basic', 'projectName empty');
  else if (proxy.projectName !== proxy.projectName.trim()) fail('basic', `projectName has whitespace: "${proxy.projectName}"`);
  if (!proxy.registrationNo) warn_('basic', 'registrationNo missing');
  if (!proxy.status) warn_('basic', 'status missing');
  if (proxy.promoterName && proxy.promoterName !== proxy.promoterName.trim()) fail('basic', `promoterName has whitespace`);

  // Financial
  const f3c = proxy.form3C || {};
  const lb  = parseFloat(f3c.loanBalance    || 0);
  const lat = parseFloat(f3c.loanAmountTaken || 0);
  if (lat > lb && lb > 0) fail('financial', `loanAmountTaken(${lat}) > loanBalance(${lb})`);
  const tec = parseFloat(f3c.totalEstimatedCost || 0);
  if (tec <= 0) warn_('financial', 'totalEstimatedCost = 0 or missing');

  // Construction
  const prog = parseFloat(proxy.overallProgress ?? -1);
  if (prog < 0 || prog > 100) fail('construction', `overallProgress=${proxy.overallProgress}`);

  // Complaints
  const cnt = parseInt(proxy.complaintCount || 0);
  const arr = proxy.complaints || [];
  if (cnt > 0 && arr.length === 0) fail('complaints', `count=${cnt} but array empty`);

  // Banks — account masking
  const banks = proxy.banks || (proxy.bank ? [proxy.bank] : []);
  const acct  = banks[0]?.accountNumber || proxy.bank?.accountNumber || '';
  if (acct && !acct.includes('×') && acct.length > 4) fail('banks', `Account not masked: "${acct}"`);

  // Legal analysis (async, only if has loan — call our own API endpoint)
  if (lb > 0) {
    try {
      const legalResp = await axios.get(
        `http://localhost:${PORT}/api/rera/project/${id}/legal-analysis`,
        { timeout: 90000 }
      );
      const legal = legalResp.data;
      const enc   = legal.encumbrance?.encumbrance_status;
      const VALID = new Set(['no_encumbrance','charge_created','partially_released','fully_released','unclear']);
      if (!VALID.has(enc)) fail('legal', `Invalid encumbrance_status: "${enc}"`);
      else {
        const net = Math.max(0, lb - lat);
        if (net > 0 && enc === 'no_encumbrance') fail('legal', `Net loan ₹${net} but enc=no_encumbrance`);
      }
    } catch(e) { warn_('legal', `Legal analysis error: ${e.message}`); }
  }

  return { id, name, issues };
}

// Helper: delete a project cache file
function cacheDelete(id) {
  try { fs.unlinkSync(path.join(CACHE_DIR, `${id}.json`)); } catch {}
}

/**
 * Main daily check routine — runs at 22:00 IST.
 * 1. Find today's new projects
 * 2. Warm cache (fetchFullProject for each)
 * 3. Run validation
 * 4. Trigger legal analysis for projects with loan data
 * 5. Write daily report JSON
 */
async function runDailyProjectCheck() {
  const startedAt = new Date().toISOString();
  console.log(`\n[DailyCheck] ===== Daily project check started at ${startedAt} =====`);

  try {
    const todayProjects = await findTodaysProjects();
    console.log(`[DailyCheck] Found ${todayProjects.length} projects registered today`);

    if (todayProjects.length === 0) {
      const report = { date: startedAt, projects_found: 0, results: [], issues_summary: 'No new projects today' };
      const fname  = path.join(DAILY_REPORT_DIR, `report_${new Date().toISOString().slice(0,10)}.json`);
      fs.writeFileSync(fname, JSON.stringify(report, null, 2));
      console.log('[DailyCheck] No new projects. Report saved:', fname);
      return;
    }

    // Validate each project (concurrency=3 to avoid hammering the proxy)
    const results = [];
    for (let i = 0; i < todayProjects.length; i += 3) {
      const batch = todayProjects.slice(i, i+3);
      const res   = await Promise.all(batch.map(async p => {
        try {
          const newHash = generateHash(p);
          const oldHash = projectHashStore.get(p.id);
          if (newHash && oldHash && newHash === oldHash) {
            console.log(`[SKIP] No change for ${p.id}`);
            return { id: p.id, name: p.name, issues: [] };
          }
          console.log(`[UPDATE] Change detected for ${p.id}`);
          const result = await validateProjectData(p.id, p.name);
          if (newHash) projectHashStore.set(p.id, newHash);
          return result;
        } catch (e) {
          // Fail-safe: hash logic error must never block normal processing
          return validateProjectData(p.id, p.name);
        }
      }));
      results.push(...res);
      await new Promise(r => setTimeout(r, 500));
    }

    const fails = results.filter(r => r.issues.some(i => i.severity === 'FAIL'));
    const warns = results.filter(r => r.issues.some(i => i.severity === 'WARN') && !r.issues.some(i=>i.severity==='FAIL'));
    const clean = results.filter(r => r.issues.length === 0);

    console.log(`[DailyCheck] Results: ${clean.length} clean | ${warns.length} warn | ${fails.length} fail`);

    if (fails.length > 0) {
      console.warn('[DailyCheck] FAILURES:');
      fails.forEach(r => {
        console.warn(`  ID ${r.id} "${r.name}"`);
        r.issues.filter(i=>i.severity==='FAIL').forEach(i => console.warn(`    ✗ [${i.section}] ${i.msg}`));
      });
    }

    const report = {
      date        : startedAt,
      since_date  : new Date().toISOString().slice(0,10),
      projects_found: todayProjects.length,
      clean, fail_count: fails.length, warn_count: warns.length,
      results,
    };
    const fname = path.join(DAILY_REPORT_DIR, `report_${new Date().toISOString().slice(0,10)}.json`);
    fs.writeFileSync(fname, JSON.stringify(report, null, 2));
    console.log(`[DailyCheck] Daily report saved: ${fname}`);
    console.log('[DailyCheck] ===== Daily check complete =====\n');
  } catch(e) {
    console.error('[DailyCheck] Fatal error:', e.message);
  }
}

/* ── Daily-check schedule (IST) ──────────────────────────────────────
   RERA office hours 10:00–19:00 IST. Runs at:
     11:00 — first-hour approvals after office opens
     14:00 — post-lunch batch
     17:00 — afternoon batch
     19:30 — 30 min after close, catches end-of-day approvals
     22:00 — nightly safety run
   ─────────────────────────────────────────────────────────────────── */
nodeCron.schedule('0 11 * * *', () => {
  console.log('[DailyCheck] Cron triggered (11:00 IST)');
  runDailyProjectCheck();
}, { timezone: 'Asia/Kolkata' });

nodeCron.schedule('0 14 * * *', () => {
  console.log('[DailyCheck] Cron triggered (14:00 IST)');
  runDailyProjectCheck();
}, { timezone: 'Asia/Kolkata' });

nodeCron.schedule('0 17 * * *', () => {
  console.log('[DailyCheck] Cron triggered (17:00 IST)');
  runDailyProjectCheck();
}, { timezone: 'Asia/Kolkata' });

nodeCron.schedule('30 19 * * *', () => {
  console.log('[DailyCheck] Cron triggered (19:30 IST)');
  runDailyProjectCheck();
}, { timezone: 'Asia/Kolkata' });

nodeCron.schedule('0 22 * * *', () => {
  console.log('[DailyCheck] Cron triggered (22:00 IST)');
  runDailyProjectCheck();
}, { timezone: 'Asia/Kolkata' });

/* Also expose a manual trigger endpoint */
app.post('/admin/run-daily-check', async (req, res) => {
  if (req.headers['x-admin-key'] !== process.env.ADMIN_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  res.json({ status: 'started', message: 'Daily project check started in background' });
  runDailyProjectCheck();  // fire-and-forget
});

app.get('/admin/daily-reports', (req, res) => {
  if (req.headers['x-admin-key'] !== process.env.ADMIN_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    const files = fs.readdirSync(DAILY_REPORT_DIR)
      .filter(f => f.endsWith('.json'))
      .sort()
      .reverse()
      .slice(0, 30);
    const reports = files.map(f => {
      const d = JSON.parse(fs.readFileSync(path.join(DAILY_REPORT_DIR, f), 'utf8'));
      return { file: f, date: d.date, projects_found: d.projects_found, fail_count: d.fail_count, warn_count: d.warn_count };
    });
    res.json({ reports });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ─────────────────────────────────────────────────────────────────
   Start
   ───────────────────────────────────────────────────────────────── */
if (require.main === module) {
  app.listen(PORT, async () => {
    console.log(`\n✅  Gujarat RERA Proxy  →  http://localhost:${PORT}`);
    console.log(`    Health:  http://localhost:${PORT}/health`);
    console.log(`    Search:  http://localhost:${PORT}/api/rera/search?q=Sun`);
    console.log(`    Detail:  http://localhost:${PORT}/api/rera/project/12\n`);
    console.log(`    Daily check: 11:00, 14:00, 17:00, 19:30, 22:00 IST | Manual: POST /admin/run-daily-check\n`);
    await buildIndex();
  });
}

/* ── Test exports (pure utility functions, no side effects) ── */
module.exports = {
  app,
  scoreText,
  maskAccountNo,
  fmtDate,
  formatRERADate,
  fmtCrore,
  getFYEndYear,
  getFilingDueDate,
  parseDDMMYY,
  calculateTimelineIndicator,
  computeRisk,
  computeSalesAlignment,
  generateLegalVerdict,
  buildReraSynthText,
  extractAmenities,
  extractArray,
  checkDownloadRateLimit,
};
