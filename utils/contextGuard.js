'use strict';

/**
 * contextGuard.js
 * Estimates token count for a prompt string and warns when it approaches
 * the safe limit for free-tier OpenRouter models used in legal analysis.
 *
 * Estimation: ~4 chars per token (standard rule-of-thumb for English/numbers).
 * Safe limit: 6000 tokens (leaves headroom for system prompt + completion).
 *
 * Usage:
 *   const { checkContextSize, truncateToLimit } = require('./contextGuard');
 *   const { safe, estimatedTokens } = checkContextSize(prompt);
 *   if (!safe) prompt = truncateToLimit(prompt);
 */

const CHARS_PER_TOKEN = 4;
const SAFE_TOKEN_LIMIT = 6000;
const HARD_TOKEN_LIMIT = 8000;    // absolute ceiling before truncation

/**
 * Estimate token count for a string.
 * @param {string} text
 * @returns {number}
 */
function estimateTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

/**
 * Check whether a prompt fits within the safe token budget.
 * @param {string} prompt
 * @returns {{ safe: boolean, estimatedTokens: number, limit: number }}
 */
function checkContextSize(prompt) {
  const estimatedTokens = estimateTokens(prompt);
  const safe = estimatedTokens <= SAFE_TOKEN_LIMIT;
  if (!safe) {
    console.warn(
      `[contextGuard] Prompt is ~${estimatedTokens} tokens — exceeds safe limit of ${SAFE_TOKEN_LIMIT}.`,
      'Consider splitting into subtasks or truncating the input.'
    );
  }
  return { safe, estimatedTokens, limit: SAFE_TOKEN_LIMIT };
}

/**
 * Truncate text so its estimated token count is at or below the hard limit.
 * Truncation happens at a word boundary to avoid mid-word cuts.
 * @param {string} text
 * @param {number} [maxTokens=HARD_TOKEN_LIMIT]
 * @returns {string}
 */
function truncateToLimit(text, maxTokens = HARD_TOKEN_LIMIT) {
  if (!text) return '';
  const maxChars = maxTokens * CHARS_PER_TOKEN;
  if (text.length <= maxChars) return text;

  // Truncate at last whitespace before the limit
  const cut = text.slice(0, maxChars);
  const lastSpace = cut.lastIndexOf(' ');
  const truncated = lastSpace > maxChars * 0.8 ? cut.slice(0, lastSpace) : cut;
  return truncated + '\n\n[...truncated to fit context limit...]';
}

/**
 * Split a long prompt into sub-prompts each under the safe limit.
 * Splits on paragraph boundaries (\n\n) where possible.
 * @param {string} text
 * @param {number} [maxTokens=SAFE_TOKEN_LIMIT]
 * @returns {string[]}
 */
function splitIntoSubtasks(text, maxTokens = SAFE_TOKEN_LIMIT) {
  if (estimateTokens(text) <= maxTokens) return [text];

  const maxChars  = maxTokens * CHARS_PER_TOKEN;
  const paragraphs = text.split(/\n\n+/);
  const subtasks  = [];
  let current     = '';

  for (const para of paragraphs) {
    const candidate = current ? current + '\n\n' + para : para;
    if (candidate.length <= maxChars) {
      current = candidate;
    } else {
      if (current) subtasks.push(current);
      // If a single paragraph exceeds the limit, hard-truncate it
      current = para.length > maxChars ? para.slice(0, maxChars) : para;
    }
  }
  if (current) subtasks.push(current);
  return subtasks;
}

module.exports = { estimateTokens, checkContextSize, truncateToLimit, splitIntoSubtasks, SAFE_TOKEN_LIMIT };
