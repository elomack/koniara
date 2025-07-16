import { Storage } from '@google-cloud/storage';
import axios from 'axios';
import pLimit from 'p-limit';

// CONFIG: Your GCP bucket name
const BUCKET_NAME = 'horse-racing-data-elomack';

// Max concurrency of parallel fetches
const CONCURRENCY_LIMIT = 10;

// Initialize Google Cloud Storage client
const storage = new Storage();

// Helper to delay execution by ms milliseconds (used for retry delays)
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * normalizeCareerData()
 *
 * Groups raw career entries by (raceYear, raceType), merging prize amounts & currencies.
 */
function normalizeCareerData(raw) {
  if (!Array.isArray(raw)) {
    console.warn('Warning: career data is not iterable:', raw);
    return [];
  }

  const map = new Map();
  for (const rec of raw) {
    const year = rec.raceYear;
    const type = rec.raceType || 'UNKNOWN';
    const key = `${year}::${type}`;
    let entry = map.get(key);

    if (!entry) {
      entry = {
        race_year:        year,
        race_type:        type,
        horse_age:        rec.horseAge || null,
        race_count:       rec.raceCount || 0,
        race_won_count:   rec.raceWonCount || 0,
        race_prize_count: rec.racePrizeCount || 0,
        prize_amounts:    [],
        prize_currencies: []
      };
      map.set(key, entry);
    }

    if (rec.prize) {
      // rec.prize format: "12345 zł" or "678 €"
      const parts = rec.prize.trim().split(/\s+/);
      const amount = parts[0];
      const currency = parts[1] || '';
      entry.prize_amounts.push(amount);
      entry.prize_currencies.push(currency);
    }
  }

  // Convert map values to array, join arrays into comma-separated strings
  return Array.from(map.values()).map(e => ({
    race_year:        e.race_year,
    race_type:        e.race_type,
    horse_age:        e.horse_age,
    race_count:       e.race_count,
    race_won_count:   e.race_won_count,
    race_prize_count: e.race_prize_count,
    prize_amounts:    e.prize_amounts.join(','),
    prize_currencies: e.prize_currencies.join(',')
  }));
}

/**
 * normalizeRacesData()
 *
 * Maps raw race entries into structured objects matching RACE_RECORDS & RACES schemas.
 */
function normalizeRacesData(raw) {
  if (!Array.isArray(raw)) {
    console.warn('Warning: races data is not iterable:', raw);
    return [];
  }

  return raw.map(r => ({
    horse_id:         r.horse?.id || null,
    race_id:          r.race?.id || null,

    // RACE_RECORDS fields
    start_order:      r.order || null,
    finish_place:     r.place || null,
    jockey_weight_kg: r.jockeyWeight || null,
    prize_amount:     r.prize || null,
    prize_currency:   r.race?.currency?.code || null,
    jockey_id:        r.jockey?.id || null,
    trainer_id:       r.trainer?.id || null,

    // RACES fields (nested race info)
    race_number:       r.race?.number || null,
    race_name:         r.race?.name || null,
    race_date:         r.race?.date || null,     // ms timestamp
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
 * Fetches `/horse/{id}`, `/horse/{id}/career`, `/horse/{id}/races` and normalizes.
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

    // Build the flattened horse object
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
      console.log(`Horse ${id} not found`);
      return null;
    }
    throw err;
  }
}

/**
 * scrapeBatch(startId, batchSize)
 *
 * Fetches a batch of horses in parallel and writes NDJSON to GCS.
 */
async function scrapeBatch(startId, batchSize) {
  if (startId <= 0 || batchSize <= 0) {
    throw new Error('startId and batchSize must be positive integers');
  }

  console.log(`Scraping horses ${startId}–${startId + batchSize - 1}`);
  const limit = pLimit(CONCURRENCY_LIMIT);
  const results = await Promise.all(
    Array.from({ length: batchSize }, (_, i) => limit(async () => {
      const id = startId + i;
      const data = await fetchHorseData(id);
      return data;
    }))
  );

  // Filter out nulls (not found) and serialize
  const valid = results.filter(r => r !== null);
  const ndjson = valid.map(r => JSON.stringify(r)).join('\n');

  const fileName = `horse_data/shard_${startId}_${startId + batchSize - 1}_${Date.now()}.ndjson`;
  const file = storage.bucket(BUCKET_NAME).file(fileName);
  await file.save(ndjson, { contentType: 'application/x-ndjson' });
  console.log(`Saved shard: gs://${BUCKET_NAME}/${fileName}`);
}

// CLI entrypoint
(async () => {
  const startId = parseInt(process.argv[2], 10) || 1;
  const batchSize = parseInt(process.argv[3], 10) || 1000;
  try {
    await scrapeBatch(startId, batchSize);
    console.log('Batch complete');
  } catch (err) {
    console.error('Error in scrapeBatch:', err);
    process.exit(1);
  }
})();
