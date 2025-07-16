import { Storage } from '@google-cloud/storage';
import axios from 'axios';

// CONFIG: Your GCP bucket name
const BUCKET_NAME = process.env.BUCKET_NAME || 'horse-predictor-v2-data';

// Max concurrency of parallel fetches and cutoff for 404s
const CONCURRENCY_LIMIT = 10;
const CUTOFF = 10;

// Initialize Google Cloud Storage client
const storage = new Storage();

// Helper: delay execution by ms milliseconds
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * formatDate()
 *
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
 * fetchBreederData(id)
 *
 * Fetches /breeder/{id} and returns normalized object or null on 404
 */
async function fetchBreederData(id) {
  try {
    const res = await axios.get(`https://homas.pkwk.org/homas/race/search/breeder/${id}`);
    const b = res.data;
    return {
      breeder_id: id,
      name:       b.name || null,
      city:       b.city || null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`⚠️ Breeder ${id} not found (404), skipping`);
      return null;
    }
    console.error(`❌ Fatal error fetching breeder ${id}:`, err);
    return null;
  }
}

/**
 * scrapeBatch(startId, batchSize)
 *
 * Uses a promise-pool to fetch breeders in parallel,
 * stops after CUTOFF consecutive misses, then writes NDJSON to GCS.
 */
async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) {
    throw new Error('❌ startId and batchSize must be positive integers');
  }
  console.debug('⏳ Scraping breeders', `${startId}–${startId + batchSize - 1}`);

  const breeders = [];
  let misses = 0;
  let nextId = startId;
  const endId = startId + batchSize;
  const active = new Set();

  // Launch one fetch and schedule next
  const launchOne = async id => {
    active.add(id);
    try {
      const rec = await fetchBreederData(id);
      if (rec) {
        misses = 0;
        console.debug('✅ Fetched breeder', id);
        breeders.push(rec);
      } else {
        misses++;
        console.debug(`⚠️ Miss #${misses} at breeder ${id}`);
      }
    } finally {
      active.delete(id);
      schedule();
    }
  };

  // Schedule up to concurrency and cutoff
  const schedule = () => {
    while (
      active.size < CONCURRENCY_LIMIT &&
      nextId < endId &&
      misses < CUTOFF
    ) {
      launchOne(nextId++);
    }
  };

  // Start initial wave
  schedule();

  // Wait for all inflight or cutoff
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

  if (breeders.length === 0) {
    console.debug('⚠️ No breeders fetched; nothing to save');
    return;
  }

  // Serialize to NDJSON
  const ndjson = breeders.map(b => JSON.stringify(b)).join('\n');
  const ts = formatDate(new Date());
  const fileName = `breeder_data/shard_${startId}_${startId + breeders.length - 1}_${ts}.ndjson`;

  console.debug('⏳ Saving shard to', fileName);
  await storage.bucket(BUCKET_NAME)
    .file(fileName)
    .save(ndjson, { contentType: 'application/x-ndjson' });

  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME}/${fileName}`);
}

// CLI entrypoint
(async () => {
  const startId = parseInt(process.argv[2], 10) || 1;
  const batchSize = parseInt(process.argv[3], 10) || 1000;
  try {
    await scrapeBatch(startId, batchSize);
    console.debug('✅ Breeder batch complete');
  } catch (err) {
    console.error('❌ Error in breeder scrapeBatch:', err);
    process.exit(1);
  }
})();
