#!/usr/bin/env node
/**
 * Cloud Run job to fetch breeder records in batches and write NDJSON to GCS.
 * Follows same conventions as horse & trainer scrapers.
 */
import { Storage } from '@google-cloud/storage';
import axios from 'axios';
import pLimit from 'p-limit';

const BUCKET_NAME       = process.env.BUCKET_NAME || 'horse-predictor-v2-data';
const PREFIX            = 'breeder_data/';
const CONCURRENCY_LIMIT = 10;
const storage = new Storage();
const delay = ms => new Promise(res => setTimeout(res, ms));

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

async function fetchBreederData(id) {
  try {
    const res = await axios.get(`https://homas.pkwk.org/homas/race/search/breeder/${id}`);
    const b = res.data;
    console.debug('✅ Fetched breeder', id);
    return {
      breeder_id: id,
      name:       b.name || null,
      city:       b.city || null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`❌ Breeder ${id} not found (404)`);
      return null;
    }
    console.error(`❌ Error fetching breeder ${id}:`, err);
    return null;
  }
}

async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) throw new Error('❌ startId and batchSize must be positive integers');
  console.debug('⏳ Scraping breeders', `${startId}–${startId + batchSize - 1}`);
  const limit = pLimit(CONCURRENCY_LIMIT);
  const results = await Promise.all(
    Array.from({ length: batchSize }, (_, idx) => limit(async () => {
      const id = startId + idx;
      return await fetchBreederData(id);
    }))
  );
  let misses = 0, endIndex = results.length;
  for (let i = 0; i < results.length; i++) {
    if (results[i] === null) misses++; else misses = 0;
    if (misses >= 10) { endIndex = i - 9; break; }
  }
  const valid = results.slice(0, endIndex).filter(r => r !== null);
  if (!valid.length) { console.debug('⏳ No breeders fetched'); return; }
  const ndjson = valid.map(o => JSON.stringify(o)).join('\n');
  const ts = formatDate(new Date());
  const fileName = `${PREFIX}shard_${startId}_${startId + batchSize - 1}_${ts}.ndjson`;
  console.debug('⏳ Saving shard to', fileName);
  await storage.bucket(BUCKET_NAME).file(fileName).save(ndjson, { contentType: 'application/x-ndjson' });
  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME}/${fileName}`);
}

(async () => {
  const startId = parseInt(process.argv[2], 10) || 1;
  const batchSize = parseInt(process.argv[3], 10) || 1000;
  try { await scrapeBatch(startId, batchSize); console.debug('✅ Breeder batch complete'); }
  catch (e) { console.error('❌ Error in breeder scrapeBatch:', e); process.exit(1); }
})();