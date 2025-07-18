// index.js for ingest Cloud Function
// Automatically discovers new cleaned NDJSON files and ingests them into BigQuery

const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize clients
const storage = new Storage();
const bigquery = new BigQuery();

// Environment variables
const BUCKET = process.env.BUCKET_NAME;                     // e.g. 'horse-predictor-v2-data'
const DATASET = process.env.BQ_DATASET;                     // e.g. 'horse_data_v2'
const METADATA_TABLE = `${bigquery.projectId}.${DATASET}.ingestion_metadata`;

// Mapping of prefixes to target tables
const prefixToTables = {
  'breeder_data/': ['BREEDERS'],
  'jockey_data/': ['JOCKEYS'],
  'trainer_data/': ['TRAINERS'],
  'horse_data/': ['HORSES', 'HORSE_CAREERS', 'RACES', 'RACE_RECORDS'],
};

exports.ingest = async (req, res) => {
  console.debug('ℹ️ ingest invoked with:', req.body);
  try {
    const { prefix } = req.body;
    if (!prefix || typeof prefix !== 'string') {
      console.warn('❗ Missing or invalid prefix');
      return res.status(400).send('Missing or invalid prefix');
    }

    // 1. Get last processed time
    const [metaRows] = await bigquery.query({
      query: `SELECT last_processed_time FROM \`${METADATA_TABLE}\` WHERE prefix = @prefix`,
      params: { prefix }
    });
    const lastProcessedTime = metaRows.length
      ? metaRows[0].last_processed_time
      : '1970-01-01T00:00:00Z';
    console.debug('Last processed time:', lastProcessedTime);

    // 2. List cleaned files in GCS
    const bucket = storage.bucket(BUCKET);
    const [files] = await bucket.getFiles({ prefix });
    const cleanedFiles = files.filter(f => f.name.startsWith(prefix + 'CLEANED_') && f.name.endsWith('.ndjson'));
    console.debug('Found cleaned files:', cleanedFiles.map(f => f.name));

    // 3. Filter new files
    const newEntries = [];
    for (const f of cleanedFiles) {
      const [meta] = await f.getMetadata();
      const createdTime = meta.timeCreated;
      if (new Date(createdTime) > new Date(lastProcessedTime)) {
        newEntries.push({ uri: `gs://${BUCKET}/${f.name}`, createdTime });
      }
    }
    if (newEntries.length === 0) {
      console.info('No new files to ingest');
      return res.status(204).send('No new files to ingest');
    }
    console.debug('New entries:', newEntries);

    // 4. For each target table: load, merge, drop staging
    for (const table of prefixToTables[prefix] || []) {
      // Determine partition suffix by latest createdTime
      const latest = newEntries.map(e => e.createdTime).sort().pop();
      const partition = latest.slice(0,10).replace(/-/g, '');
      const stagingId = `stg_${table}_${partition}`;
      const stagingRef = `${bigquery.projectId}.${DATASET}.${stagingId}`;
      const prodRef = `${bigquery.projectId}.${DATASET}.${table}`;
      const uris = newEntries.map(e => e.uri);

      // 4a. Load NDJSON into staging
      console.debug('Loading into staging:', stagingRef, uris);
      const [loadJob] = await bigquery.createJob({
        configuration: {
          load: {
            destinationTable: {
              projectId: bigquery.projectId,
              datasetId: DATASET,
              tableId: stagingId
            },
            sourceUris: uris,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE'
          }
        }
      });
      await loadJob.promise();
      console.info('Loaded into staging:', stagingRef);

      // 4b. Merge into production
      let mergeSql = '';
      switch (table) {
        case 'BREEDERS':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.breeder_id=S.breeder_id WHEN MATCHED THEN UPDATE SET name=S.name,city=S.city WHEN NOT MATCHED THEN INSERT(breeder_id,name,city) VALUES(S.breeder_id,S.name,S.city)`;
          break;
        case 'JOCKEYS':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.jockey_id=S.jockey_id WHEN MATCHED THEN UPDATE SET first_name=S.first_name,last_name=S.last_name,licence_country=S.licence_country WHEN NOT MATCHED THEN INSERT(jockey_id,first_name,last_name,licence_country) VALUES(S.jockey_id,S.first_name,S.last_name,S.licence_country)`;
          break;
        case 'TRAINERS':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.trainer_id=S.trainer_id WHEN MATCHED THEN UPDATE SET first_name=S.first_name,last_name=S.last_name,licence_country=S.licence_country WHEN NOT MATCHED THEN INSERT(trainer_id,first_name,last_name,licence_country) VALUES(S.trainer_id,S.first_name,S.last_name,S.licence_country)`;
          break;
        case 'HORSES':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.horse_id=S.horse_id WHEN MATCHED THEN UPDATE SET horse_name=S.horse_name,horse_country=S.horse_country,birth_year=S.birth_year,horse_sex=S.horse_sex,breed=S.breed,mother_id=S.mother_id,father_id=S.father_id,trainer_id=S.trainer_id,breeder_id=S.breeder_id,color_name_pl=S.color_name_pl,color_name_en=S.color_name_en,polish_breeding=S.polish_breeding,foreign_training=S.foreign_training,owner_name=S.owner_name WHEN NOT MATCHED THEN INSERT(horse_id,horse_name,horse_country,birth_year,horse_sex,breed,mother_id,father_id,trainer_id,breeder_id,color_name_pl,color_name_en,polish_breeding,foreign_training,owner_name) VALUES(S.horse_id,S.horse_name,S.horse_country,S.birth_year,S.horse_sex,S.breed,S.mother_id,S.father_id,S.trainer_id,S.breeder_id,S.color_name_pl,S.color_name_en,S.polish_breeding,S.foreign_training,S.owner_name)`;
          break;
        case 'HORSE_CAREERS':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.horse_id=S.horse_id AND T.race_year=S.race_year AND T.race_type=S.race_type WHEN MATCHED THEN UPDATE SET horse_age=S.horse_age,race_count=S.race_count,race_won_count=S.race_won_count,race_prize_count=S.race_prize_count,prize_amounts=S.prize_amounts,prize_currencies=S.prize_currencies WHEN NOT MATCHED THEN INSERT(horse_id,race_year,race_type,horse_age,race_count,race_won_count,race_prize_count,prize_amounts,prize_currencies) VALUES(S.horse_id,S.race_year,S.race_type,S.horse_age,S.race_count,S.race_won_count,S.race_prize_count,S.prize_amounts,S.prize_currencies)`;
          break;
        case 'RACES':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.race_id=S.race_id WHEN MATCHED THEN UPDATE SET race_number=S.race_number,race_name=S.race_name,race_date=S.race_date,currency_code=S.currency_code,currency_symbol=S.currency_symbol,duration_ms=S.duration_ms,track_distance_m=S.track_distance_m,temperature_c=S.temperature_c,weather=S.weather,race_group=S.race_group,subtype=S.subtype,category_id=S.category_id,category_breed=S.category_breed,category_name=S.category_name,country_code=S.country_code,city_name=S.city_name,track_type=S.track_type,video_url=S.video_url,race_rules=S.race_rules,payments=S.payments,race_style=S.race_style WHEN NOT MATCHED THEN INSERT(race_id,race_number,race_name,race_date,currency_code,currency_symbol,duration_ms,track_distance_m,temperature_c,weather,race_group,subtype,category_id,category_breed,category_name,country_code,city_name,track_type,video_url,race_rules,payments,race_style) VALUES(S.race_id,S.race_number,S.race_name,S.race_date,S.currency_code,S.currency_symbol,S.duration_ms,S.track_distance_m,S.temperature_c,S.weather,S.race_group,S.subtype,S.category_id,S.category_breed,S.category_name,S.country_code,S.city_name,S.track_type,S.video_url,S.race_rules,S.payments,S.race_style)`;
          break;
        case 'RACE_RECORDS':
          mergeSql = `MERGE \`${prodRef}\` T USING \`${stagingRef}\` S ON T.race_record_id=S.race_record_id WHEN MATCHED THEN UPDATE SET race_id=S.race_id,horse_id=S.horse_id,start_order=S.start_order,finish_place=S.finish_place,jockey_weight_kg=S.jockey_weight_kg,prize_amount=S.prize_amount,prize_currency=S.prize_currency,jockey_id=S.jockey_id,trainer_id=S.trainer_id WHEN NOT MATCHED THEN INSERT(race_record_id,race_id,horse_id,start_order,finish_place,jockey_weight_kg,prize_amount,prize_currency,jockey_id,trainer_id) VALUES(S.race_record_id,S.race_id,S.horse_id,S.start_order,S.finish_place,S.jockey_weight_kg,S.prize_amount,S.prize_currency,S.jockey_id,S.trainer_id)`;
          break;
        default:
          console.warn(`No merge logic for ${table}`);
          continue;
      }
      console.debug('Merging:', prodRef);
      await bigquery.query({ query: mergeSql });
      console.info('Merged:', table);

      // 4c. Drop staging table
      const dropSql = `DROP TABLE \`${stagingRef}\``;
      await bigquery.query({ query: dropSql });
      console.info('Dropped staging:', stagingRef);
    }

    // 5. Update metadata table
    const newLatest = newEntries.map(e => e.createdTime).sort().pop();
    const upsertSql = `MERGE \`${METADATA_TABLE}\` M USING (SELECT @prefix AS prefix, @ts AS last_processed_time) N ON M.prefix=N.prefix WHEN MATCHED THEN UPDATE SET last_processed_time=N.last_processed_time WHEN NOT MATCHED THEN INSERT(prefix, last_processed_time) VALUES(N.prefix,N.last_processed_time)`;
    await bigquery.query({ query: upsertSql, params: { prefix, ts: newLatest } });
    console.info('Updated metadata for', prefix);

    return res.status(200).json({ prefix, count: newEntries.length, lastProcessedTime: newLatest });
  } catch (err) {
    console.error('ingest error:', err);
    res.status(500).send(`ingest failed: ${err.message}`);
  }
};
