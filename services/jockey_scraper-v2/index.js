#!/usr/bin/env node
/**
 * jockey_scraper.js
 *
 * Cloud Run job to fetch jockey records in batches and write NDJSON to GCS.
 */
import { Storage } from '@google-cloud/storage';
import axios from 'axios';
import pLimit from 'p-limit';

const BUCKET_NAME_J   = process.env.BUCKET_NAME || 'horse-predictor-v2-data';
const PREFIX_J        = 'jockey_data/';
const CONCURRENCY_J   = 10;
const storageJ        = new Storage();
const delayJ = ms => new Promise(res => setTimeout(res, ms));

function formatDateJ(date) {
  const pad = n => String(n).padStart(2, '0');
  const YYYY = date.getFullYear();
  const MM   = pad(date.getMonth() + 1);
  const DD   = pad(date.getDate());
  const hh   = pad(date.getHours());
  const mm   = pad(date.getMinutes());
  const ss   = pad(date.getSeconds());
  return `${YYYY}_${MM}_${DD}_${hh}:${mm}:${ss}`;
}

async function fetchJockeyData(id) {
  try {
    const res = await axios.get(`https://homas.pkwk.org/homas/race/search/jockey/${id}`);
    console.debug('✅ Fetched jockey', id);
    const j = res.data;
    return {
      jockey_id:      id,
      first_name:     j.firstName || null,
      last_name:      j.lastName  || null,
      licence_country:j.licenceCountry?.alfa3 || null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`❌ Jockey ${id} not found (404)`);
      return null;
    }
    console.error(`❌ Error fetching jockey ${id}:`, err);
    return null;
  }
}

async function scrapeBatchJ(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) throw new Error('❌ startId and batchSize must be positive integers');
  console.debug('⏳ Scraping jockeys', `${startId}–${startId + batchSize - 1}`);

  const jockeys = [];
  let misses = 0;
  let nextId = startId;
  const endId = startId + batchSize;
  const active = new Set();

  // Launch one fetch and schedule next
  const launchOne = async id => {
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
      active.size < CONCURRENCY_J &&
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
        console.warn(`⚠️ Stopped after several consecutive misses at ID ${nextId - 1}`);
        return resolve();
      } else if (nextId >= endId && active.size === 0) {
        return resolve();
      }
      setTimeout(check, 100);
    };
    check();
  });

  if (jockeys.length === 0) {
    console.debug('⚠️ No jockeys fetched');
    return;
  }

  const ndjson = jockeys.map(o => JSON.stringify(o)).join('');
  const ts = formatDateJ(new Date());
  const fileName = `${PREFIX_J}shard_${startId}_${startId + jockeys.length - 1}_${ts}.ndjson`;

  console.debug('⏳ Saving shard to', fileName);
  await storageJ.bucket(BUCKET_NAME_J).file(fileName)
    .save(ndjson, { contentType: 'application/x-ndjson' });

  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME_J}/${fileName}`);
}  
  const valid = results.slice(0,endIndex).filter(r=>r!==null);
  if(!valid.length){console.debug('⏳ No jockeys fetched');return;}
  const ndjson = valid.map(o=>JSON.stringify(o)).join('\n');
  const ts = formatDateJ(new Date());
  const fileName = `${PREFIX_J}shard_${startId}_${startId + batchSize - 1}_${ts}.ndjson`;
  console.debug('⏳ Saving shard to', fileName);
  await storageJ.bucket(BUCKET_NAME_J).file(fileName).save(ndjson,{contentType:'application/x-ndjson'});
  console.debug('✅ Saved shard:',`gs://${BUCKET_NAME_J}/${fileName}`);


(async()=>{
  const startId = parseInt(process.argv[2],10)||1;
  const batchSize = parseInt(process.argv[3],10)||1000;
  try{ await scrapeBatchJ(startId,batchSize); console.debug('✅ Jockey batch complete'); }
  catch(e){ console.error('❌ Error in jockey scrapeBatch:',e); process.exit(1);} 
})();
