'use strict';

const crypto = require('crypto');

/**
 * Generate a SHA-256 hash from a fixed set of project fields.
 * Only the fields listed below are hashed — unrelated data changes are ignored.
 * Returns null (not a string) on any error so callers can treat null as "no prior hash".
 */
function generateHash(data) {
  try {
    const fields = {
      status         : data.status           ?? null,
      completion_date: data.completion_date   ?? null,
      complaints     : data.complaints        ?? null,
      last_updated   : data.last_updated      ?? null,
      docs_length    : data.documents?.length ?? null,
      units_sold     : data.units_sold        ?? null,
      total_units    : data.total_units       ?? null,
    };
    return crypto.createHash('sha256').update(JSON.stringify(fields)).digest('hex');
  } catch (e) {
    return null;
  }
}

module.exports = { generateHash };
