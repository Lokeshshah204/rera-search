'use strict';

/**
 * batchProcessor.js
 * Splits an array of items into fixed-size batches, processes batches
 * sequentially, and runs items within each batch with bounded parallelism.
 *
 * Usage:
 *   const { processBatches } = require('./batchProcessor');
 *   const results = await processBatches(projectIds, analyzeProject, {
 *     batchSize  : 20,   // items per batch
 *     concurrency: 5,    // parallel workers inside each batch
 *     delayMs    : 500,  // ms pause between batches (rate-limit cushion)
 *     onBatchDone: (batchNo, total, batchResults) => { ... }
 *   });
 */

const pLimit = require('p-limit');

/**
 * @param {any[]}    items        Full list to process
 * @param {Function} workerFn     async fn(item) → result
 * @param {object}   [opts]
 * @param {number}   [opts.batchSize=20]
 * @param {number}   [opts.concurrency=5]
 * @param {number}   [opts.delayMs=0]
 * @param {Function} [opts.onBatchDone]  called after each batch with (batchNo, totalBatches, batchResults)
 * @returns {Promise<{item:any, result?:any, error?:Error}[]>}
 */
async function processBatches(items, workerFn, opts = {}) {
  const {
    batchSize   = 20,
    concurrency = 5,
    delayMs     = 0,
    onBatchDone = null,
  } = opts;

  const allResults = [];

  const batches     = chunk(items, batchSize);
  const totalBatches = batches.length;

  for (let b = 0; b < batches.length; b++) {
    const batch   = batches[b];
    const limit   = pLimit(concurrency);
    const tasks   = batch.map(item =>
      limit(async () => {
        try {
          const result = await workerFn(item);
          return { item, result };
        } catch (error) {
          return { item, error };
        }
      })
    );

    const batchResults = await Promise.all(tasks);
    allResults.push(...batchResults);

    if (typeof onBatchDone === 'function') {
      onBatchDone(b + 1, totalBatches, batchResults);
    }

    // Pause between batches (not after the last one)
    if (delayMs > 0 && b < batches.length - 1) {
      await new Promise(r => setTimeout(r, delayMs));
    }
  }

  return allResults;
}

/**
 * Split array into fixed-size chunks.
 * @param {any[]} arr
 * @param {number} size
 * @returns {any[][]}
 */
function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

module.exports = { processBatches, chunk };
