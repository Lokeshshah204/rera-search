'use strict';

/**
 * retryFetch.js
 * Axios-based HTTP helpers with automatic 429 retry (up to 3 attempts, 30s wait).
 * Drop-in replacement for the raw axiosGet / axiosPost calls in proxy-server.js.
 */

const axios     = require('axios');
const https     = require('https');
const constants = require('constants');

const SSL_AGENT = new https.Agent({
  secureOptions: constants.SSL_OP_LEGACY_SERVER_CONNECT,
  rejectUnauthorized: false,
});

const DEFAULT_HEADERS = {
  'User-Agent'     : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept'         : 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Referer'        : 'https://gujrera.gujarat.gov.in/',
  'Origin'         : 'https://gujrera.gujarat.gov.in',
};

const MAX_RETRIES      = 3;
const RETRY_DELAY_MS   = 30_000;   // 30 seconds on 429
const BACKOFF_CODES    = new Set([429, 503]);

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * GET with retry.
 * @param {string} url
 * @param {number} [timeout=12000]
 * @param {object} [extraHeaders={}]
 * @returns {Promise<any>} response.data
 */
async function retryGet(url, timeout = 12000, extraHeaders = {}) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const r = await axios.get(url, {
        httpsAgent: SSL_AGENT,
        headers   : { ...DEFAULT_HEADERS, ...extraHeaders },
        timeout,
      });
      return r.data;
    } catch (err) {
      lastErr = err;
      const status = err?.response?.status;
      if (BACKOFF_CODES.has(status) && attempt < MAX_RETRIES) {
        console.warn(`[retryFetch] GET ${url} → ${status}. Waiting ${RETRY_DELAY_MS / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(RETRY_DELAY_MS);
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

/**
 * POST with retry.
 * @param {string} url
 * @param {any}    body
 * @param {number} [timeout=15000]
 * @param {object} [extraHeaders={}]
 * @returns {Promise<any>} response.data
 */
async function retryPost(url, body, timeout = 15000, extraHeaders = {}) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const r = await axios.post(url, body, {
        httpsAgent: SSL_AGENT,
        headers   : { ...DEFAULT_HEADERS, 'Content-Type': 'application/json', ...extraHeaders },
        timeout,
      });
      return r.data;
    } catch (err) {
      lastErr = err;
      const status = err?.response?.status;
      if (BACKOFF_CODES.has(status) && attempt < MAX_RETRIES) {
        console.warn(`[retryFetch] POST ${url} → ${status}. Waiting ${RETRY_DELAY_MS / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(RETRY_DELAY_MS);
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

module.exports = { retryGet, retryPost, SSL_AGENT, DEFAULT_HEADERS };
