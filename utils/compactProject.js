'use strict';

/**
 * compactProject.js
 * Extracts only the fields needed for AI analysis / scan reporting from a
 * full RERA project response object.  Avoids sending multi-KB raw API payloads
 * to OpenRouter — typically reduces payload by 90%+.
 *
 * Usage:
 *   const { toCompact } = require('./compactProject');
 *   const compact = toCompact(fullProjectResponse);
 */

/**
 * @param {object} p  Full project response from /api/rera/project/:id
 * @returns {object}  Compact representation for analysis
 */
function toCompact(p) {
  if (!p || typeof p !== 'object') return {};

  return {
    projectId           : p.projectId           ?? null,
    projectName         : p.projectName         ?? null,
    promoterName        : p.promoterName        ?? null,
    status              : p.status              ?? null,
    district            : p.district            ?? null,
    processType         : p.processType         ?? null,

    // Dates
    projectStartDate    : p.projectStartDate    ?? null,
    originalCompletionDate : p.originalCompletionDate ?? null,
    effectiveCompletionDate: p.effectiveCompletionDate ?? null,

    // Construction
    overallProgress     : p.overallProgress     ?? null,
    timelineIndicator   : p.timelineIndicator
      ? {
          indicator      : p.timelineIndicator.indicator,
          progressGap    : p.timelineIndicator.progressGap,
          extensionGranted: p.timelineIndicator.extensionGranted,
        }
      : null,

    // Units
    totalUnits          : p.totalUnits          ?? null,
    bookedUnits         : p.bookedUnits         ?? null,

    // QPR compliance
    qprFiled            : p.qprFiled            ?? null,
    qprRequired         : p.qprRequired         ?? null,
    qprDefaultedCount   : p.qprDefaultedCount   ?? null,

    // Financials (Form 3C)
    loanBalance         : p.form3C?.loanBalance         ?? null,
    loanAmountTaken     : p.form3C?.loanAmountTaken     ?? null,
    totalEstimatedCost  : p.form3C?.totalEstimatedCost  ?? null,
    totalCostIncurred   : p.form3C?.totalCostIncurred   ?? null,

    // Complaints
    complaints          : Array.isArray(p.complaints)
      ? p.complaints.length
      : (p.complaints ?? 0),

    // Risk
    riskScore           : p.riskScore           ?? null,

    // Annual compliance — just pass/fail counts
    annualComplianceSummary: _annualSummary(p.annualCompliance),

    // Extensions count
    extensionCount      : Array.isArray(p.extensions) ? p.extensions.length : 0,
  };
}

function _annualSummary(arr) {
  if (!Array.isArray(arr) || arr.length === 0) return null;
  const filed    = arr.filter(r => r.status && r.status.trim()).length;
  const total    = arr.length;
  return { filed, total };
}

module.exports = { toCompact };
