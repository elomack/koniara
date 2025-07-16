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
  const limit = pLimit(CONCURRENCY_J);
  const results = await Promise.all(
    Array.from({ length: batchSize }, (_, idx) => limit(async () => fetchJockeyData(startId + idx)))
  );
  let misses=0, endIndex = results.length;
  for (let i=0;i<results.length;i++){
    if(results[i]===null) misses++; else misses=0;
    if(misses>=10){ endIndex=i-9; break; }
  }
  const valid = results.slice(0,endIndex).filter(r=>r!==null);
  if(!valid.length){console.debug('⏳ No jockeys fetched');return;}
  const ndjson = valid.map(o=>JSON.stringify(o)).join('\n');
  const ts = formatDateJ(new Date());
  const fileName = `${PREFIX_J}shard_${startId}_${startId + batchSize - 1}_${ts}.ndjson`;
  console.debug('⏳ Saving shard to', fileName);
  await storageJ.bucket(BUCKET_NAME_J).file(fileName).save(ndjson,{contentType:'application/x-ndjson'});
  console.debug('✅ Saved shard:',`gs://${BUCKET_NAME_J}/${fileName}`);
}

(async()=>{
  const startId = parseInt(process.argv[2],10)||1;
  const batchSize = parseInt(process.argv[3],10)||1000;
  try{ await scrapeBatchJ(startId,batchSize); console.debug('✅ Jockey batch complete'); }
  catch(e){ console.error('❌ Error in jockey scrapeBatch:',e); process.exit(1);} 
})();
