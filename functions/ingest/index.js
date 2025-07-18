// index.js for ingest Cloud Function
// Discovers new cleaned NDJSON files, flattens horse_data arrays, and ingests into BigQuery

const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const readline = require('readline');

// Initialize clients
const storage = new Storage();
const bigquery = new BigQuery();

// Environment variables
const BUCKET = process.env.BUCKET_NAME;
const DATASET = process.env.BQ_DATASET;
const METADATA_TABLE = `${bigquery.projectId}.${DATASET}.ingestion_metadata`;

// Prefix → tables mapping
const prefixToTables = {
  'breeder_data/': ['BREEDERS'],
  'jockey_data/': ['JOCKEYS'],
  'trainer_data/': ['TRAINERS'],
  // horse_data special-handled
  'horse_data/': ['HORSES', 'HORSE_CAREERS', 'RACES', 'RACE_RECORDS'],
};

exports.ingest = async (req, res) => {
  console.info('ℹ️ ingest invoked with:', req.body);
  try {
    const { prefix } = req.body;
    if (!prefix || typeof prefix !== 'string') {
      console.warn('❗ Missing or invalid prefix');
      return res.status(400).send('Missing or invalid prefix');
    }

    // 1. Get watermark
    const [metaRows] = await bigquery.query({
      query: `SELECT last_processed_time FROM \`${METADATA_TABLE}\` WHERE prefix=@prefix`,
      params: { prefix },
    });
    const lastProcessedTime = metaRows.length
      ? metaRows[0].last_processed_time
      : '1970-01-01T00:00:00Z';
    console.debug('Last processed time:', lastProcessedTime);

    // 2. List cleaned files
    const bucket = storage.bucket(BUCKET);
    const [files] = await bucket.getFiles({ prefix });
    const cleaned = files.filter(f =>
      f.name.startsWith(prefix) && f.name.includes('CLEANED_') && f.name.endsWith('.ndjson')
    );

    // 3. Filter new files
    const newFiles = [];
    for (const f of cleaned) {
      const [meta] = await f.getMetadata();
      if (new Date(meta.timeCreated) > new Date(lastProcessedTime)) {
        newFiles.push({ file: f, createdTime: meta.timeCreated });
      }
    }
    if (newFiles.length === 0) {
      console.info('⚠️ No new files to ingest');
      return res.status(204).send('No new files to ingest');
    }

    // 4. Handle horse_data prefix with flatten
    if (prefix === 'horse_data/') {
      const horses = [];
      const careers = [];
      const races = [];
      const records = [];
      const seenRaceIds = new Set();

      for (const entry of newFiles) {
        const stream = entry.file.createReadStream();
        const rl = readline.createInterface({ input: stream });
        for await (const line of rl) {
          let obj;
          try { obj = JSON.parse(line); } catch { continue; }
          // HORSES
          horses.push({
            horse_id: obj.horse_id,
            horse_name: obj.horse_name,
            horse_country: obj.horse_country || null,
            birth_year: obj.birth_year,
            horse_sex: obj.horse_sex,
            breed: obj.breed,
            mother_id: obj.mother_id || null,
            father_id: obj.father_id || null,
            trainer_id: obj.trainer_id,
            breeder_id: obj.breeder_id,
            color_name_pl: obj.color_name_pl,
            color_name_en: obj.color_name_en,
            polish_breeding: obj.polish_breeding,
            foreign_training: obj.foreign_training,
            owner_name: obj.owner_name,
          });
          // HORSE_CAREERS
          for (const c of obj.career || []) {
            careers.push({
              horse_id: obj.horse_id,
              race_year: c.race_year,
              race_type: c.race_type,
              horse_age: c.horse_age,
              race_count: c.race_count,
              race_won_count: c.race_won_count,
              race_prize_count: c.race_prize_count,
              prize_amounts: c.prize_amounts,
              prize_currencies: c.prize_currencies,
            });
          }
          // RACES & RACE_RECORDS
          for (const r of obj.races || []) {
            if (!seenRaceIds.has(r.race_id)) {
              races.push({
                race_id: r.race_id,
                race_number: r.race_number,
                race_name: r.race_name,
                race_date: new Date(r.race_date),
                currency_code: r.prize_currency,
                currency_symbol: r.currency_symbol || null,
                duration_ms: r.duration_ms || null,
                track_distance_m: r.track_distance_m,
                temperature_c: r.temperature_c,
                weather: r.weather,
                race_group: r.race_group,
                subtype: r.subtype,
                category_id: r.category_id,
                category_breed: r.category_breed,
                category_name: r.category_name,
                country_code: r.country_code,
                city_name: r.city_name,
                track_type: r.track_type,
                video_url: r.video_url || null,
                race_rules: r.race_rules || null,
                payments: r.payments || null,
                race_style: r.race_style,
              });
              seenRaceIds.add(r.race_id);
            }
            records.push({
              race_record_id: r.race_record_id,
              race_id: r.race_id,
              horse_id: obj.horse_id,
              start_order: r.start_order,
              finish_place: r.finish_place,
              jockey_weight_kg: r.jockey_weight_kg,
              prize_amount: r.prize_amount || null,
              prize_currency: r.prize_currency,
              jockey_id: r.jockey_id,
              trainer_id: r.trainer_id,
            });
          }
        }
      }
      // Insert flattened data
      await Promise.all([
        bigquery.dataset(DATASET).table('HORSES').insert(horses),
        bigquery.dataset(DATASET).table('HORSE_CAREERS').insert(careers),
        bigquery.dataset(DATASET).table('RACES').insert(races),
        bigquery.dataset(DATASET).table('RACE_RECORDS').insert(records),
      ]);
      console.info('✅ horse_data flattened and ingested');
    } else {
      // 5. Reference tables: staging, merge, drop
      const refTables = prefixToTables[prefix] || [];
      for (const table of refTables) {
        const dateSuffix = newFiles
          .map(e => e.createdTime)
          .sort()
          .pop()
          .slice(0,10)
          .replace(/-/g,'');
        const stagingId = `stg_${table}_${dateSuffix}`;
        const stagingRef = `${bigquery.projectId}.${DATASET}.${stagingId}`;
        const prodRef = `${bigquery.projectId}.${DATASET}.${table}`;
        const uris = newFiles.map(e => `gs://${BUCKET}/${e.file.name}`);

        console.debug(`⬆️ Loading into staging ${stagingRef}`);
        const [loadJob] = await bigquery.createJob({
          configuration: {
            load: {
              destinationTable: { projectId: bigquery.projectId, datasetId: DATASET, tableId: stagingId },
              sourceUris: uris,
              sourceFormat: 'NEWLINE_DELIMITED_JSON',
              autodetect: true,
              writeDisposition: 'WRITE_TRUNCATE',
            }
          }
        });
        await loadJob.promise();
        console.info(`✅ Loaded into staging ${stagingRef}`);

        // Merge SQL logic
        let mergeSql = '';
        if (table === 'BREEDERS') {
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S
               ON T.breeder_id=S.breeder_id
               WHEN MATCHED THEN UPDATE SET name=S.name,city=S.city
               WHEN NOT MATCHED THEN INSERT(breeder_id,name,city) VALUES(S.breeder_id,S.name,S.city)`;
        } else if (table === 'JOCKEYS') {
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S
               ON T.jockey_id=S.jockey_id
               WHEN MATCHED THEN UPDATE SET first_name
= S.first_name,last_name=S.last_name,licence_country=S.licence_country
               WHEN NOT MATCHED THEN INSERT(jockey_id,first_name,last_name,licence_country)
               VALUES(S.jockey_id,S.first_name,S.last_name,S.licence_country)`;
        } else if (table === 'TRAINERS') {
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S
               ON T.trainer_id=S.trainer_id
               WHEN MATCHED THEN UPDATE SET first_name=S.first_name,last_name
=S.last_name,licence_country=S.licence_country
               WHEN NOT MATCHED THEN INSERT(trainer_id,first_name,last_name,licence_country)
               VALUES(S.trainer_id,S.first_name,S.last_name,S.licence_country)`;
        }
        console.debug(`↗️ Merging into ${prodRef}`);
        await bigquery.query({ query: mergeSql });
        console.info(`✅ Merged ${table}`);

        console.debug(`🗑️ Dropping staging ${stagingRef}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingRef}\`` });
        console.info(`✅ Dropped staging ${stagingRef}`);
      }
    }

    // 6. Update watermark
    // Determine the latest processed timestamp as a JavaScript Date
    const latestString = newFiles.map(e => e.createdTime).sort().pop();
    const newLatest = new Date(latestString);
    await bigquery.query({
      query: `MERGE \`${METADATA_TABLE}\` M USING (SELECT @prefix AS prefix, @ts AS last_processed_time) N ON M.prefix=N.prefix WHEN MATCHED THEN UPDATE SET last_processed_time=N.last_processed_time WHEN NOT MATCHED THEN INSERT(prefix,last_processed_time) VALUES(N.prefix,N.last_processed_time)`,
      params: { prefix, ts: newLatest },
    });
    console.info('✅ Updated metadata');

    return res.status(200).json({ prefix, count: newFiles.length, lastProcessedTime: newLatest });
  } catch (err) {
    console.error('❌ ingest error:', err);
    return res.status(500).send(`ingest failed: ${err.message}`);
  }
};
