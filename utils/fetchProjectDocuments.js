'use strict';

/**
 * fetchProjectDocuments.js
 * Fetches all available document UIDs for a RERA project and returns a
 * structured list with labels, filenames, and VDMS download URLs.
 *
 * Strategy:
 *   1. Fetch the projectdoc endpoint.
 *   2. Extract the 7 known explicit fields (guaranteed correct).
 *   3. Also sweep ALL keys in the projectdoc response whose name ends with
 *      a UID suffix (DocUId, UId, Uid) — catches any additional documents
 *      the portal may expose that aren't in our hardcoded list.
 *   4. Deduplicate by UID value.
 */

const { retryGet } = require('./retryFetch');

const RERA_BASE       = 'https://gujrera.gujarat.gov.in';
const PROJECT_DOC_API = `${RERA_BASE}/project_reg/public/getproject-doc/`;
const VDMS_DOWNLOAD   = `${RERA_BASE}/vdms/download/`;

/* Keys that look like UID fields but are structural IDs, never document UIDs */
const SKIP_KEYS = new Set([
  'formOneId','formTwoId','formThreeId','promoterId','promotorId',
  'projectId','prjRegId','quarterId','qusrterId','blockId','unitId',
  'userId','engId','appId','applicationId','id','ID',
]);

/**
 * Returns true if a value could be a VDMS document UID.
 * Key insight: we only call this for keys whose NAME already ends in a UID
 * suffix, so we just need to reject obvious non-UID values (dates, empty, spaces).
 */
function isDocValue(val) {
  if (!val || typeof val !== 'string') return false;
  const s = val.trim();
  if (!s || s.length < 3) return false;
  if (/\s/.test(s)) return false;                          // no spaces
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;        // YYYY-MM-DD
  if (/^\d{2}[\/\-]\d{2}[\/\-]\d{4}$/.test(s)) return false; // DD/MM/YYYY
  return true;
}

/** Convert a camelCase key to a readable label by stripping UID suffixes. */
function keyToLabel(key) {
  let s = key
    .replace(/DocUId$|DocUID$|DocUid$/i, '')
    .replace(/_?[Uu][Ii][Dd]$/,          '')
    .replace(/_?[Ii][Dd]$/,              '');
  if (!s) return null;
  // camelCase → words
  s = s.replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
       .replace(/([a-z\d])([A-Z])/g, '$1 $2')
       .replace(/_/g, ' ').trim();
  return s.split(' ').filter(Boolean)
          .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
          .join(' ');
}

const RENAME_MAP = [
  { test: /encumbran/i,                            label: 'Land Encumbrance Certificate'     },
  { test: /title/i,                                label: 'Title Report'                     },
  { test: /form\s*3|formthree|ca\s*cert/i,         label: 'CA Certified Cost Statement'      },
  { test: /reg(istration)?\s*(cert|certific)/i,    label: 'Project Registration Certificate' },
  { test: /layout|approved\s*layout|sanctioned/i,  label: 'Sanctioned Layout Plan'           },
  { test: /building\s*perm|^bp$/i,                 label: 'Building Use Permission'          },
  { test: /architect/i,                            label: 'Architect Completion Certificate' },
  { test: /form\s*1\b|formone\b/i,                 label: 'Form 1 Declaration'               },
  { test: /form\s*2\b|formtwo\b/i,                 label: 'Form 2 Declaration'               },
  { test: /form\s*5\b|formfive\b/i,                label: 'Form 5 Declaration'               },
  { test: /possession/i,                           label: 'Possession Certificate'           },
  { test: /qpr|quarter/i,                          label: 'QPR Progress Document'            },
];

function applyRename(raw) {
  if (!raw) return raw;
  for (const { test, label } of RENAME_MAP) {
    if (test.test(raw)) return label;
  }
  return raw;
}

/**
 * @param {number|string} projectId
 * @returns {Promise<Array<{uid:string, label:string, filename:string, url:string}>>}
 */
async function fetchProjectDocuments(projectId) {
  const seenUids = new Set();
  const docs     = [];

  function push(uid, label, filename) {
    if (!uid || seenUids.has(uid)) return;
    seenUids.add(uid);
    docs.push({ uid, label, filename, url: `${VDMS_DOWNLOAD}${uid}` });
  }

  try {
    const resp = await retryGet(`${PROJECT_DOC_API}${projectId}`, 12000);
    const pd   = resp?.data?.projectdoc || resp?.projectdoc || {};

    /* ── Phase 1: explicit known fields (guaranteed correct) ── */
    push(pd.encumbranceCertificateDocUId,
         'Land Encumbrance Certificate', 'Land_Encumbrance_Certificate.pdf');
    push(pd.titleReportUId,
         'Title Report', 'Title_Report.pdf');
    push(pd.form3DocUId || pd.formThreeDocUId,
         'CA Certified Cost Statement', 'CA_Certified_Cost_Statement.pdf');
    push(pd.registrationCertificateDocUId || pd.regCertDocUId,
         'Project Registration Certificate', 'Project_Registration_Certificate.pdf');
    push(pd.layoutPlanDocUId || pd.approvedLayoutDocUId,
         'Sanctioned Layout Plan', 'Sanctioned_Layout_Plan.pdf');
    push(pd.buildingPermissionDocUId || pd.bpDocUId,
         'Building Use Permission', 'Building_Use_Permission.pdf');
    push(pd.architectCertificateDocUId || pd.archCertDocUId,
         'Architect Completion Certificate', 'Architect_Completion_Certificate.pdf');

    /* ── Phase 2: sweep remaining keys in projectdoc for any additional docs ── */
    for (const [key, val] of Object.entries(pd)) {
      if (SKIP_KEYS.has(key)) continue;
      const kl = key.toLowerCase();
      // Only process keys whose name indicates a document UID
      const isUidKey = kl.endsWith('docuid') || kl.endsWith('docuuid') ||
                       (kl.endsWith('uid') && !kl.endsWith('guid')) ||
                       kl.includes('docuid');
      if (!isUidKey) continue;
      if (!isDocValue(val)) continue;
      const raw   = keyToLabel(key);
      const label = applyRename(raw) || raw || key;
      const fname = label.replace(/\s+/g, '_').replace(/[^A-Za-z0-9_-]/g, '') + '.pdf';
      push(val.trim(), label, fname);
    }

  } catch (err) {
    console.warn('[Docs] Failed to fetch document list:', err.message);
  }

  return docs;
}

module.exports = fetchProjectDocuments;
