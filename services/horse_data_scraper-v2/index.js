import { Storage } from '@google-cloud/storage';
import axios from 'axios';

// FX rates to PLN
const FX_RATES = {
  'pln': 1,
  'zł': 1,
  'zl': 1,
  'eur': 4.25,
  '€': 4.25,
  'Kč': 0.18,      // approximate CZK→PLN
  'czk': 0.18,
  'Skr': 0.42,     // SEK→PLN
  'Ft': 0.011,     // HUF→PLN
  'huf': 0.011,
  'AED': 1.16,     // AED→PLN
  'aed': 1.16,     // AED→PLN
  '$': 4.0,        // USD→PLN
};

// CONFIG: Your GCP bucket name
const BUCKET_NAME = process.env.BUCKET_NAME || 'horse-predictor-v2-data';

// Max concurrency of parallel fetches
const CONCURRENCY_LIMIT = 10;
// How many consecutive 404s before giving up
const CUTOFF = 10;

// Initialize Google Cloud Storage client
const storage = new Storage();

// Helper to delay execution by ms milliseconds (used for retry delays)
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * formatDate()
 *
 * Formats a Date object as YYYY_MM_DD_hh:mm:ss
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
 * normalizeCareerData()
 *
 * Groups raw career entries by (raceYear, raceType), merging prize amounts & currencies.
 * Detects prize-only records (raceYear === null) and merges into the last real bucket.
 */
function normalizeCareerData(raw) {
  if (!Array.isArray(raw)) {
    console.debug('⚠️ Warning: career data is not iterable');
    return [];
  }
  const map = new Map();
  let lastKey = null;
  for (const rec of raw) {
    if (rec.raceYear !== null) {
      const type = rec.raceType || 'UNKNOWN';
      const key  = `${rec.raceYear}::${type}`;
      if (!map.has(key)) {
        map.set(key, {
          race_year:        rec.raceYear,
          race_type:        rec.raceType || null,
          horse_age:        rec.horseAge || null,
          race_count:       rec.raceCount || 0,
          race_won_count:   rec.raceWonCount || 0,
          race_prize_count: rec.racePrizeCount || 0,
          prize_amounts:    0,       // total in PLN
          prize_currencies: 'PLN'    // always PLN
        });
      }
      lastKey = key;
    } else if (lastKey) {
      const entry = map.get(lastKey);
      if (rec.prize) {
        const parts = rec.prize.trim().split(/\s+/);
        const rawAmount = parseFloat(parts[0].replace(',', '.')) || 0;
        const curr = (parts[1] || 'pln').toLowerCase();
        // Lookup rate (default to PLN if unknown)
        const rate = FX_RATES[curr] || 1;
        if (!FX_RATES.hasOwnProperty(curr)) {
          console.warn("⚠️ Unmapped currency '" + parts[1] + "', defaulting rate=1.");
        }
        const amountPLN = rawAmount * rate;
        entry.prize_amounts += amountPLN;
      }
    }
  }
  return Array.from(map.values()).map(e => ({
    race_year:        e.race_year,
    race_type:        e.race_type,
    horse_age:        e.horse_age,
    race_count:       e.race_count,
    race_won_count:   e.race_won_count,
    race_prize_count: e.race_prize_count,
    prize_amounts:    e.prize_amounts,      // numeric total in PLN
    prize_currencies: e.prize_currencies    // always 'PLN'
  }));
}

/**
 * normalizeRacesData()
 *
 * Maps raw race entries into structured objects matching RACE_RECORDS & RACES schemas.
 */
function normalizeRacesData(raw) {
  if (!Array.isArray(raw)) {
    console.debug('⚠️ Warning: races data is not iterable');
    return [];
  }
  return raw.map(r => ({
    horse_id:         r.horse?.id || null,
    race_id:          r.race?.id || null,
    start_order:      r.order || null,
    finish_place: (typeof r.place === 'number' && r.place > 0)
    ? r.place
    : 'UNKNOWN',    jockey_weight_kg: r.jockeyWeight || null,
    prize_amount:     r.prize || null,
    prize_currency:   r.race?.currency?.code || null,
    jockey_id:        r.jockey?.id || null,
    trainer_id:       r.trainer?.id || null,
    race_number:       r.race?.number || null,
    race_name:         r.race?.name || null,
    video_url:    r.race?.video || null,
    race_date:         r.race?.date || null,
    track_distance_m:  r.race?.trackDistance || null,
    temperature_c:     r.race?.temperature || null,
    weather:           r.race?.weather || null,
    race_group:        r.race?.group || null,
    subtype:           r.race?.subType || null,
    category_id:       r.race?.category?.id || null,
    category_breed:    r.race?.category?.horseBreed || null,
    category_name:     r.race?.category?.name || null,
    country_code:      r.race?.country?.alfa3 || null,
    city_name:         r.race?.city?.name || null,
    track_type:        r.race?.trackType?.name || null,
    race_rules:        r.race?.fullConditions || null,
    payments:          r.race?.payments || null,
    race_style:        r.race?.style?.name || null
  }));
}

/**
 * fetchHorseData(id)
 *
 * Fetches horse details and normalizes career & races.
 */
async function fetchHorseData(id) {
  try {
    const res = await axios.get(`https://homas.pkwk.org/homas/race/search/horse/${id}`);
    const horse = res.data;
    const careerRes = await axios.get(
      `https://homas.pkwk.org/homas/race/search/horse/${id}/career`
    );
    const careerData = normalizeCareerData(careerRes.data.data);
    const racesRes = await axios.get(
      `https://homas.pkwk.org/homas/race/search/horse/${id}/races`
    );
    const racesData = normalizeRacesData(racesRes.data);
    return {
      horse_id:           id,
      horse_name:         horse.name || null,
      horse_country:      horse.suffix || null,
      birth_year:         horse.dateOfBirth || null,
      horse_sex:          horse.sex || null,
      breed:              horse.breed || null,
      mother_id:          horse.mother?.id || null,
      father_id:          horse.father?.id || null,
      trainer_id:         horse.trainer?.id || null,
      breeder_id:         horse.breeders?.[0]?.id || null,
      color_name_pl:      horse.color?.polishName || null,
      color_name_en:      horse.color?.englishName || null,
      polish_breeding:    horse.horseFromPolishBreeding || false,
      foreign_training:   horse.horseRanInForeignTraining || false,
      owner_name:         horse.raceOwners?.[0]?.name || null,
      career:             careerData,
      races:              racesData
    };
  } catch (err) {
    if (err.response?.status === 404) {
      console.debug(`⚠️ Horse ${id} not found (404), skipping`);
      return null;
    }
    console.error(`❌ Fatal error fetching horse data for horse ${id}:`, err);
    return null;
  }
}

/**
 * scrapeBatch(startId, batchSize)
 *
 * Fetches a batch of horses with a promise-pool, stops after CUTOFF misses,
 * and writes NDJSON to GCS.
 */
async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) {
    throw new Error('❌ startId and batchSize must be positive integers');
  }
  console.debug('⏳ Scraping horses', `${startId}–${startId + batchSize - 1}`);

  const horses = [];
  let misses = 0;
  let nextId = startId;
  const endId = startId + batchSize;
  const active = new Set();

  // Launch a single fetch and then schedule next
  const launchOne = async (id) => {
    active.add(id);
    try {
      const rec = await fetchHorseData(id);
      if (rec) {
        misses = 0;
        console.debug('✅ Fetched horse', id);
        horses.push(rec);
      } else {
        misses++;
      }
    } finally {
      active.delete(id);
      schedule();
    }
  };

  // Fill up to CONCURRENCY_LIMIT
  const schedule = () => {
    while (
      active.size < CONCURRENCY_LIMIT &&
      nextId < endId &&
      misses < CUTOFF
    ) {
      launchOne(nextId++);
    }
  };

  // Start initial batch
  schedule();

  // Wait for completion or cutoff
  await new Promise(resolve => {
    const check = () => {
      if (misses >= CUTOFF) {
        console.warn(`⚠️ Stopped after several consecutive misses at ID ${nextId - 1}`);
        resolve();
      } else if (nextId >= endId && active.size === 0) {
        resolve();
      } else {
        setTimeout(check, 100);
      }
    };
    check();
  });

  if (horses.length === 0) {
    console.debug('⚠️ No horses fetched; nothing to save');
    return;
  }

  const ndjson = horses.map(r => JSON.stringify(r)).join('\n');
  const ts = formatDate(new Date());
  const fileName = `horse_data/shard_${startId}_${startId + horses.length - 1}_${ts}.ndjson`;

  console.debug('⏳ Saving shard to', fileName);
  const file = storage.bucket(BUCKET_NAME).file(fileName);
  await file.save(ndjson, { contentType: 'application/x-ndjson' });

  console.debug('✅ Saved shard:', `gs://${BUCKET_NAME}/${fileName}`);
}

// CLI entrypoint
(async () => {
  const startId = parseInt(process.argv[2], 10);
  const batchSize = parseInt(process.argv[3], 10);
  try {
    await scrapeBatch(startId, batchSize);
    console.debug('✅ Batch complete');
  } catch (err) {
    console.error('❌ Error in scrapeBatch:', err);
    process.exit(1);
  }
})();
