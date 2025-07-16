#!/usr/bin/env node
/**
 * trainer_scraper.js
 *
 * Cloud Run job to fetch trainer records in batches and write NDJSON to GCS.
 *
 * Usage:
 *   node index.js <startId> <batchSize>
 *
 * Steps:
 *   1) For each trainer ID from startId to startId+batchSize-1, call the Trainer API
 *   2) On 404, skip. After 10 consecutive 404s, assume end-of-table and truncate the batch
 *   3) Collect valid trainer objects, serialize to NDJSON
 *   4) Save NDJSON to GCS at gs://BUCKET_NAME/trainer_data/shard_<start>_<end>_<timestamp>.ndjson
 */

import { Storage } from '@google-cloud/storage';
import axios from 'axios';
import pLimit from 'p-limit';

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────
const BUCKET_NAME       = process.env.BUCKET_NAME || 'horse-predictor-v2-data';
const PREFIX            = 'trainer_data/';        // GCS folder prefix
const CONCURRENCY_LIMIT = 10;                      // parallel fetches before backpressure

// Initialize GCS client
const storage = new Storage();

// Helper: delay execution by ms milliseconds
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * formatDate()
 *
 * Formats a Date as YYYY_MM_DD_hh:mm:ss for human-readable filenames
 */
function formatDate(date) {
  const pad = n => String(n).padStart(2, '0');
  const YYYY = date.getFullYear();
  const MM   = pad(date.getMonth() + 1);
  const DD   = pad(date.getDate());
  const hh   = pad(date.getHours());
  const mm   = pad(date.getMinutes());
  const ss   = pad(date.getSeconds());
  return `${YYYY}_${MM}_${DD}_${hh}:${mm}:${ss}`;
}

/**
 * fetchTrainerData(id)
 *
 * Fetches /trainer/{id} and returns a normalized object, or null if 404.
 */
async function fetchTrainerData(id) {
  try {
    const res = await axios.get(
      `https://homas.pkwk.org/homas/race/search/trainer/${id}`
    );
    const t = res.data;
    // Debug raw response to ensure correct field
    console.error(`🎯 Raw licenceCountry for trainer ${id}:`, t.licenceCountry);

    // Extract ISO3 code from nested licenceCountry object (correct camelCase)
    const iso3 = t.licenceCountry?.alfa3 || null;
    console.error(`🎯 Mapped licence_country for trainer ${id}:`, iso3);

    return {
      trainer_id:      id,
      first_name:      t.firstName || null,
      last_name:       t.lastName  || null,
      licence_country: t.licenceCountry?.alfa3 || null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`❌ Trainer ${id} not found (404)`);
      return null;
    }
    throw err;
  }
}

/**
 * scrapeBatch(startId, batchSize)
 *
 * Orchestrates the fetch of a batch of trainers and writes to GCS.
 */
async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) {
    throw new Error('❌ startId and batchSize must be positive integers');
  }

  console.debug('⏳ Scraping trainers', `${startId}–${startId + batchSize - 1}`);
  const limit   = pLimit(CONCURRENCY_LIMIT);
  const results = await Promise.all(
    Array.from({ length: batchSize }, (_, idx) => limit(async () => {
      const id = startId + idx;
      try {
        const data = await fetchTrainerData(id);
        console.debug('✅ Fetched trainer', id);
        return data;
      } catch (err) {
        console.error('❌ Error in fetchTrainerData for', id, err);
        return null;
      }
    }))
  );

  // Detect 10 consecutive 404s => assume end of table
  let consecutiveMisses = 0;
  let endIndex = results.length;
  for (let i = 0; i < results.length; i++) {
    if (results[i] === null) {
      consecutiveMisses++;
    } else {
      consecutiveMisses = 0;
    }
    if (consecutiveMisses >= 10) {
      const idReached = startId + i;
      console.warn(`⚠️ Detected 10 consecutive misses up to trainer ${idReached}, stopping batch.`);
      endIndex = i - 9;
      break;
    }
  }
  const truncated = results.slice(0, endIndex);
  const valid     = truncated.filter(r => r !== null);
  if (valid.length === 0) {
    console.debug('⏳ No trainer records fetched in this batch.');
    return;
  }

  // Serialize to NDJSON
  const ndjson = valid.map(obj => JSON.stringify(obj)).join('\n');

  // Build filename with human-readable timestamp
  const ts       = formatDate(new Date());
  const fileName = `${PREFIX}shard_${startId}_${startId + batchSize - 1}_${ts}.ndjson`;
  const file     = storage.bucket(BUCKET_NAME).file(fileName);

  console.debug('⏳ Saving shard to', fileName);
  await file.save(ndjson, { contentType: 'application/x-ndjson' });
  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME}/${fileName}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI entrypoint
// ─────────────────────────────────────────────────────────────────────────────
(async () => {
  const startId   = parseInt(process.argv[2], 10) || 1;
  const batchSize = parseInt(process.argv[3], 10) || 1000;
  try {
    await scrapeBatch(startId, batchSize);
    console.debug('✅ Trainer batch complete');
  } catch (err) {
    console.error('❌ Error in trainer scrapeBatch:', err);
    process.exit(1);
  }
})();
