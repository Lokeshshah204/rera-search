'use strict';

// ─── Date Helpers ────────────────────────────────────────────────────────────

function parseDate(s) {
  if (!s || s === 'Not Available' || s === 'Pending') return null;
  const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(`${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`);
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

function monthsBetween(a, b) {
  if (!a || !b) return null;
  return (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth());
}

// ─── Compact Payload Builder ──────────────────────────────────────────────────

function buildCompact(d) {
  // Booking %
  const bBooked = d.unitBookingFromBlocks?.booked
    ?? parseInt(d.form3A?.bookedUnits || d.unitSummary?.bookedUnits || 0);
  const bTotal  = d.unitBookingFromBlocks?.total
    ?? parseInt(d.form3A?.totalUnits  || d.unitSummary?.totalUnits  || 0);
  const bookingPercentage = bTotal > 0
    ? parseFloat((bBooked / bTotal * 100).toFixed(1))
    : null;

  // QPR — exclude BWA entries per CLAUDE.md
  const qprRows    = (d.qprHistory || []).filter(q => !String(q.quarterName || '').startsWith('BWA'));
  const qprFiled   = qprRows.filter(q => q.isDefault !== 'Y' && q.isDefault !== true).length;
  const qprRequired = qprRows.length;

  // Financials
  const loanBalance      = parseFloat(d.form3C?.loanBalance       || 0);
  const loanAmountTaken  = parseFloat(d.form3C?.loanAmountTaken   || 0);
  const totalEstimatedCost = parseFloat(
    d.form3C?.totalEstimatedCost || d.totalEstimatedCost || 0
  );

  // Total units
  const totalUnits = bTotal || 0;

  return {
    projectId            : d.projectId,
    projectName          : d.projectName,
    district             : d.district,
    projectStartDate     : d.projectStartDate,
    originalCompletionDate: d.originalCompletionDate || d.completionDate,
    currentCompletionDate : d.completionDate,
    bookingPercentage,
    constructionProgress : d.overallProgress ?? 0,
    qprFiled,
    qprRequired,
    complaintsCount      : (d.complaints || []).length,
    totalUnits,
    loanBalance,
    loanAmountTaken,
    totalEstimatedCost,
  };
}

// ─── Standard Metrics ────────────────────────────────────────────────────────

function calculateStandardMetrics(compact) {
  const {
    projectStartDate, originalCompletionDate,
    bookingPercentage, constructionProgress,
    loanBalance, loanAmountTaken, totalEstimatedCost,
    complaintsCount, totalUnits,
  } = compact;

  const startDate = parseDate(projectStartDate);
  const endDate   = parseDate(originalCompletionDate);
  const today     = new Date();

  const elapsedMonths = monthsBetween(startDate, today);
  const totalMonths   = monthsBetween(startDate, endDate);

  const timeProgress = (elapsedMonths != null && totalMonths != null && totalMonths > 0)
    ? parseFloat((Math.min(100, Math.max(0, elapsedMonths) / totalMonths * 100).toFixed(1)))
    : null;

  const salesVsConstructionGap = bookingPercentage != null
    ? parseFloat((bookingPercentage - constructionProgress).toFixed(1))
    : null;

  const constructionVsTimeGap = timeProgress != null
    ? parseFloat((constructionProgress - timeProgress).toFixed(1))
    : null;

  const netLoan = Math.max(0, loanBalance - loanAmountTaken);

  const loanExposurePct = (totalEstimatedCost > 0)
    ? parseFloat((netLoan / totalEstimatedCost * 100).toFixed(2))
    : null;

  const complaintRatio = (totalUnits > 0)
    ? parseFloat((complaintsCount / totalUnits * 100).toFixed(2))
    : null;

  return {
    timeProgress,
    salesVsConstructionGap,
    constructionVsTimeGap,
    netLoan,
    loanExposurePct,
    complaintRatio,
  };
}

// ─── Indicator Helpers ────────────────────────────────────────────────────────

const SCORE = { 'On Track': 1, 'Monitor': 2, 'Attention': 3 };

function toIndicator(score) {
  if (score <= 1.5)  return 'On Track';
  if (score <= 2.2)  return 'Monitor';
  return 'Attention';
}

// ─── Module 1: Construction Timeline ─────────────────────────────────────────

function constructionTimelineModule(compact, metrics) {
  const { constructionProgress } = compact;
  const { timeProgress, constructionVsTimeGap } = metrics;

  if (timeProgress == null) {
    return {
      title          : 'Construction Timeline',
      keyMetrics     : [
        { label: 'Construction Progress', value: `${constructionProgress}%` },
        { label: 'Time Elapsed',          value: '—' },
        { label: 'Build vs Schedule',     value: '—' },
      ],
      indicator      : 'Monitor',
      observationText: 'Project start or completion date not available. Timeline comparison cannot be computed.',
      confidence     : 'Low',
    };
  }

  let indicator;
  if (constructionVsTimeGap >= -10)       indicator = 'On Track';
  else if (constructionVsTimeGap >= -25)  indicator = 'Monitor';
  else                                    indicator = 'Attention';

  const fmtGap = v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%';

  return {
    title      : 'Construction Timeline',
    keyMetrics : [
      { label: 'Construction Progress', value: `${constructionProgress}%` },
      { label: 'Time Elapsed',          value: `${timeProgress}%` },
      { label: 'Build vs Schedule',     value: fmtGap(constructionVsTimeGap) },
    ],
    indicator,
    observationText: indicator === 'On Track'
      ? 'Reported construction progress is broadly consistent with the elapsed project timeline.'
      : indicator === 'Monitor'
        ? 'Reported construction progress appears somewhat lower than the timeline implied by the original completion date.'
        : 'Reported construction progress appears lower than the timeline implied by the original completion date. Users may review updated timelines and any RERA-approved extensions.',
    confidence: 'High',
  };
}

// ─── Module 2: Sales vs Construction ─────────────────────────────────────────

function salesVsConstructionModule(compact, metrics) {
  const { bookingPercentage, constructionProgress } = compact;
  const { salesVsConstructionGap } = metrics;

  if (bookingPercentage == null) {
    return {
      title          : 'Sales vs Construction',
      keyMetrics     : [
        { label: 'Units Booked',       value: '—' },
        { label: 'Construction',       value: `${constructionProgress}%` },
        { label: 'Sales vs Build Gap', value: '—' },
      ],
      indicator      : 'Monitor',
      observationText: 'Booking data not available for this project.',
      confidence     : 'Low',
    };
  }

  let indicator;
  if (salesVsConstructionGap > 30)       indicator = 'Attention';
  else if (salesVsConstructionGap > 10)  indicator = 'Monitor';
  else                                   indicator = 'On Track';

  const fmtGap = v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%';

  return {
    title      : 'Sales vs Construction',
    keyMetrics : [
      { label: 'Units Booked',       value: `${bookingPercentage}%` },
      { label: 'Construction',       value: `${constructionProgress}%` },
      { label: 'Sales vs Build Gap', value: fmtGap(salesVsConstructionGap) },
    ],
    indicator,
    observationText: indicator === 'On Track'
      ? 'Units booked relative to construction progress are broadly consistent, as reported in RERA filings.'
      : `${bookingPercentage}% of units are reported as booked against ${constructionProgress}% construction progress, as per RERA filings. Users may review fund utilisation details for context.`,
    confidence: 'High',
  };
}

// ─── Module 3: QPR Compliance ─────────────────────────────────────────────────

function complianceModule(compact) {
  const { qprFiled, qprRequired } = compact;

  if (qprRequired === 0) {
    return {
      title          : 'Quarterly Compliance (QPR)',
      keyMetrics     : [
        { label: 'QPRs Required', value: '0' },
        { label: 'QPRs Filed',    value: '0' },
        { label: 'Pending',       value: '0' },
      ],
      indicator      : 'Monitor',
      observationText: 'No quarterly progress report history found in RERA records.',
      confidence     : 'Low',
    };
  }

  const qprGap = qprRequired - qprFiled;

  let indicator;
  if (qprGap === 0)     indicator = 'On Track';
  else if (qprGap <= 2) indicator = 'Monitor';
  else                  indicator = 'Attention';

  return {
    title      : 'Quarterly Compliance (QPR)',
    keyMetrics : [
      { label: 'QPRs Required', value: String(qprRequired) },
      { label: 'QPRs Filed',    value: String(qprFiled) },
      { label: 'Pending',       value: String(qprGap) },
    ],
    indicator,
    observationText: qprGap === 0
      ? `All ${qprRequired} quarterly progress reports are filed as per RERA records.`
      : `Some quarterly filings are pending as per RERA records. ${qprGap} of ${qprRequired} QPR(s) not filed on time.`,
    confidence: 'High',
  };
}

// ─── Module 4: Complaints ─────────────────────────────────────────────────────

function complaintsModule(compact, metrics) {
  const { complaintsCount, totalUnits } = compact;
  const { complaintRatio } = metrics;

  let indicator;
  if (complaintRatio == null) {
    indicator = complaintsCount === 0 ? 'On Track' : 'Monitor';
  } else if (complaintRatio < 2)   indicator = 'On Track';
  else if (complaintRatio <= 5)    indicator = 'Monitor';
  else                             indicator = 'Attention';

  return {
    title      : 'Buyer Complaints',
    keyMetrics : [
      { label: 'Complaints Filed', value: String(complaintsCount) },
      { label: 'Total Units',      value: totalUnits > 0 ? String(totalUnits) : '—' },
      { label: 'Complaint Ratio',  value: complaintRatio != null ? `${complaintRatio}%` : '—' },
    ],
    indicator,
    observationText: complaintsCount === 0
      ? 'No complaints are recorded against this project in the Gujarat RERA database.'
      : `${complaintsCount} complaint${complaintsCount !== 1 ? 's are' : ' is'} recorded in the Gujarat RERA database. These represent formal filings before the RERA authority. Users may review details for context.`,
    confidence: totalUnits > 0 ? 'High' : 'Medium',
  };
}

// ─── Module 5: Financial Exposure ────────────────────────────────────────────

function financialExposureModule(compact, metrics) {
  const { loanBalance, loanAmountTaken, totalEstimatedCost } = compact;
  const { netLoan, loanExposurePct } = metrics;

  // Skip if no form3C data
  if (loanBalance === 0 && loanAmountTaken === 0 && totalEstimatedCost === 0) {
    return {
      title          : 'Financial Exposure',
      keyMetrics     : [
        { label: 'Net Loan Outstanding', value: '—' },
        { label: 'Total Project Cost',   value: '—' },
        { label: 'Loan Exposure',        value: '—' },
      ],
      indicator      : 'Monitor',
      observationText: 'Financial data from Form 3C is not available for this project.',
      confidence     : 'Low',
    };
  }

  const fmtCr = v => {
    if (v >= 1e7) return `₹${(v / 1e7).toFixed(2)} Cr`;
    if (v >= 1e5) return `₹${(v / 1e5).toFixed(2)} L`;
    return `₹${v.toFixed(0)}`;
  };

  let indicator;
  if (loanExposurePct == null)        indicator = 'Monitor';
  else if (loanExposurePct < 20)      indicator = 'On Track';
  else if (loanExposurePct <= 40)     indicator = 'Monitor';
  else                                indicator = 'Attention';

  return {
    title      : 'Financial Exposure',
    keyMetrics : [
      { label: 'Net Loan Outstanding', value: fmtCr(netLoan) },
      { label: 'Total Project Cost',   value: fmtCr(totalEstimatedCost) },
      { label: 'Loan Exposure',        value: loanExposurePct != null ? `${loanExposurePct}%` : '—' },
    ],
    indicator,
    observationText: 'Outstanding project loan exposure is derived from Form 3C disclosures filed with Gujarat RERA.',
    confidence: totalEstimatedCost > 0 ? 'High' : 'Medium',
  };
}

// ─── Aggregation ──────────────────────────────────────────────────────────────

const SUMMARY_TEXT =
  'This summary is based on publicly disclosed RERA data and is intended to assist users in reviewing project disclosures.';

function getProjectInsights(d) {
  try {
    const compact = buildCompact(d);
    const metrics = calculateStandardMetrics(compact);

    const modules = [
      constructionTimelineModule(compact, metrics),
      salesVsConstructionModule(compact, metrics),
      complianceModule(compact),
      complaintsModule(compact, metrics),
      financialExposureModule(compact, metrics),
    ];

    // Aggregate — exclude Low confidence modules from scoring
    const scorable = modules.filter(m => m.confidence !== 'Low');
    const overallScore = scorable.length > 0
      ? scorable.reduce((sum, m) => sum + SCORE[m.indicator], 0) / scorable.length
      : 2; // default to Monitor when no scorable modules

    const overallIndicator = toIndicator(overallScore);

    console.log(`[DecisionEngine] projectId=${compact.projectId} overall=${overallIndicator} score=${overallScore.toFixed(2)} modules=${modules.length} scorable=${scorable.length}`);

    return { overallIndicator, modules, summaryText: SUMMARY_TEXT };
  } catch (err) {
    console.error('[DecisionEngine] Error computing insights:', err.message);
    return null;
  }
}

module.exports = { getProjectInsights };
