#!/usr/bin/env node
/**
 * jockey_scraper.js
 *
 * Cloud Run job to fetch jockey records in batches and write NDJSON to GCS.
 */
import { Storage } from '@google-cloud/storage';
import axios from 'axios';

// CONFIGURATION
const BUCKET_NAME = process.env.BUCKET_NAME || 'horse-predictor-v2-data';
const PREFIX      = 'jockey_data/';
const CONCURRENCY = 10;   // parallel fetch limit
const CUTOFF      = 10;   // consecutive 404s before stopping

// Initialize GCS client
const storage = new Storage();

// Helper: delay execution by ms
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * formatDate()
 * Formats a Date as YYYY_MM_DD_hh:mm:ss for filenames
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
 * fetchJockeyData(id)
 * Fetches /jockey/{id} and returns normalized object, or null on 404.
 */
async function fetchJockeyData(id) {
  try {
    const res = await axios.get(`https://homas.pkwk.org/homas/race/search/jockey/${id}`);
    const j = res.data;
    console.debug('✅ Fetched jockey', id);
    return {
      jockey_id:      id,
      first_name:     j.firstName || null,
      last_name:      j.lastName  || null,
      licence_country:j.licenceCountry?.alfa3 || null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`⚠️ Jockey ${id} not found (404), skipping`);
      return null;
    }
    console.error(`❌ Error fetching jockey ${id}:`, err);
    return null;
  }
}

/**
 * scrapeBatch(startId, batchSize)
 * Fetches a batch of jockeys in parallel, stops after CUTOFF consecutive misses,
 * then writes NDJSON to GCS.
 */
async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) {
    throw new Error('❌ startId and batchSize must be positive integers');
  }
  console.debug('⏳ Scraping jockeys', `${startId}–${startId + batchSize - 1}`);

  const jockeys = [];
  let misses = 0;
  let nextId = startId;
  const endId = startId + batchSize;
  const active = new Set();

  // Launch a fetch and schedule the next one
  const launchOne = async (id) => {
    active.add(id);
    try {
      const rec = await fetchJockeyData(id);
      if (rec) {
        misses = 0;
        jockeys.push(rec);
      } else {
        misses++;
        console.debug(`⚠️ Miss #${misses} at jockey ${id}`);
      }
    } finally {
      active.delete(id);
      schedule();
    }
  };

  // Schedule up to concurrency and cutoff
  const schedule = () => {
    while (
      active.size < CONCURRENCY &&
      nextId < endId &&
      misses < CUTOFF
    ) {
      launchOne(nextId++);
    }
  };

  // Start initial wave
  schedule();

  // Wait until all done or cutoff reached
  await new Promise(resolve => {
    const check = () => {
      if (misses >= CUTOFF) {
        console.warn(`⚠️ Stopped after ${CUTOFF} consecutive misses at ID ${nextId - 1}`);
        return resolve();
      } else if (nextId >= endId && active.size === 0) {
        return resolve();
      }
      setTimeout(check, 100);
    };
    check();
  });

  if (jockeys.length === 0) {
    console.debug('⚠️ No jockeys fetched; nothing to save');
    return;
  }

  // Serialize to NDJSON and save
  const ndjson = jockeys.map(o => JSON.stringify(o)).join('\n');
  const ts = formatDate(new Date());
  const fileName = `${PREFIX}shard_${startId}_${startId + jockeys.length - 1}_${ts}.ndjson`;

  console.debug('⏳ Saving shard to', fileName);
  await storage.bucket(BUCKET_NAME).file(fileName)
    .save(ndjson, { contentType: 'application/x-ndjson' });

  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME}/${fileName}`);
}

// CLI entrypoint
(async () => {
  const startId   = parseInt(process.argv[2], 10) || 1;
  const batchSize = parseInt(process.argv[3], 10) || 1000;
  try {
    await scrapeBatch(startId, batchSize);
    console.debug('✅ Jockey batch complete');
  } catch (err) {
    console.error('❌ Error in jockey scrapeBatch:', err);
    process.exit(1);
  }
})();
