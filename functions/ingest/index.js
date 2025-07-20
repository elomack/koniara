// index.js for ingest Cloud Function
// Discovers new cleaned NDJSON files and ingests into BigQuery with upsert semantics

const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize clients
const storage = new Storage();
const bigquery = new BigQuery();

// Environment variables
const BUCKET = process.env.BUCKET_NAME;
const DATASET = process.env.BQ_DATASET;
const METADATA_TABLE = `${bigquery.projectId}.${DATASET}.ingestion_metadata`;

// Prefix → reference tables mapping
const prefixToTables = {
  'breeder_data/': ['BREEDERS'],
  'jockey_data/': ['JOCKEYS'],
  'trainer_data/': ['TRAINERS'],
};

exports.ingest = async (req, res) => {
  console.info('ℹ️ ingest invoked with:', req.body);
  try {
    const { prefix } = req.body;
    if (!prefix || typeof prefix !== 'string') {
      console.warn('❗ Missing or invalid prefix');
      return res.status(400).send('Missing or invalid prefix');
    }

    // 1. Load watermark
    const [metaRows] = await bigquery.query({
      query: `SELECT last_processed_time FROM \`${METADATA_TABLE}\` WHERE prefix=@prefix`,
      params: { prefix }
    });
    const lastProcessedTime = metaRows.length > 0
      ? new Date(metaRows[0].last_processed_time.value)
      : new Date('1970-01-01T00:00:00Z');
    console.debug('Last processed time:', lastProcessedTime.toISOString());

    // 2. List and filter cleaned files
    const bucket = storage.bucket(BUCKET);
    const [files] = await bucket.getFiles({ prefix });
    const newFiles = [];
    for (const f of files) {
      if (!f.name.includes('CLEANED_') || !f.name.endsWith('.ndjson')) continue;
      const [meta] = await f.getMetadata();
      const created = new Date(meta.timeCreated);
      if (created > lastProcessedTime) {
        console.debug(`🆕 New cleaned file: ${f.name}`);
        newFiles.push({ file: f, created });
      }
    }
    if (newFiles.length === 0) {
      console.info('⚠️ No new files to ingest');
      return res.status(204).send('No new files to ingest');
    }

    // 3. Branch by prefix
    if (prefix === 'horse_data/') {
      for (const entry of newFiles) {
        const dateSuffix = entry.created.toISOString().slice(0,10).replace(/-/g,'');
        const stagingId = `raw_horse_data_${dateSuffix}`;
        const stagingTable = `${bigquery.projectId}.${DATASET}.${stagingId}`;
        const uri = `gs://${BUCKET}/${entry.file.name}`;

        console.debug(`⬆️ Loading raw JSON staging ${stagingTable}`);
        const [loadJob] = await bigquery.createJob({
          configuration: {
            load: {
              destinationTable: { projectId: bigquery.projectId, datasetId: DATASET, tableId: stagingId },
              sourceUris: [uri],
              sourceFormat: 'NEWLINE_DELIMITED_JSON',
              autodetect: true,
              writeDisposition: 'WRITE_TRUNCATE'
            }
          }
        });
        await loadJob.promise();
        console.info(`✅ Loaded raw staging ${stagingTable}`);

        // MERGE HORSES
        {
          const sql = `
            MERGE \`${DATASET}.HORSES\` T
            USING (
              SELECT
                CAST(horse_id AS INT64) AS horse_id,
                horse_name,
                horse_country,
                CAST(birth_year AS INT64) AS birth_year,
                horse_sex,
                breed,
                CAST(mother_id AS INT64) AS mother_id,
                CAST(father_id AS INT64) AS father_id,
                CAST(trainer_id AS INT64) AS trainer_id,
                CAST(breeder_id AS INT64) AS breeder_id,
                color_name_pl,
                color_name_en,
                polish_breeding,
                foreign_training,
                owner_name
              FROM \`${DATASET}.${stagingId}\`
            ) S
            ON T.horse_id = S.horse_id
            WHEN MATCHED THEN
              UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT(
                horse_id, horse_name, horse_country, birth_year, horse_sex,
                breed, mother_id, father_id, trainer_id, breeder_id,
                color_name_pl, color_name_en, polish_breeding, foreign_training,
                owner_name, created_date, last_updated_date
              ) VALUES (
                S.horse_id, S.horse_name, S.horse_country, S.birth_year, S.horse_sex,
                S.breed, S.mother_id, S.father_id, S.trainer_id, S.breeder_id,
                S.color_name_pl, S.color_name_en, S.polish_breeding, S.foreign_training,
                S.owner_name, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
          `;
          console.debug('↗️ MERGE into HORSES');
          const [job] = await bigquery.createQueryJob({ query: sql });
          await job.getQueryResults();
          const stats = job.metadata.statistics.dmlStats || {};
          console.info(`✅ HORSES MERGE: inserted=${stats.insertedRowCount||0}, updated=${stats.updatedRowCount||0}`);
        }

        // MERGE HORSE_CAREERS
        {
          const sql = `
            MERGE \`${DATASET}.HORSE_CAREERS\` T
            USING (
              SELECT
                CAST(st.horse_id AS INT64) AS horse_id,
                CAST(JSON_EXTRACT_SCALAR(c_item, '$.race_year') AS INT64) AS race_year,
                JSON_EXTRACT_SCALAR(c_item, '$.race_type') AS race_type,
                CAST(JSON_EXTRACT_SCALAR(c_item, '$.horse_age') AS INT64) AS horse_age,
                CAST(JSON_EXTRACT_SCALAR(c_item, '$.race_count') AS INT64) AS race_count,
                CAST(JSON_EXTRACT_SCALAR(c_item, '$.race_won_count') AS INT64) AS race_won_count,
                CAST(JSON_EXTRACT_SCALAR(c_item, '$.race_prize_count') AS INT64) AS race_prize_count,
                JSON_EXTRACT_SCALAR(c_item, '$.prize_amounts') AS prize_amounts,
                JSON_EXTRACT_SCALAR(c_item, '$.prize_currencies') AS prize_currencies
              FROM \`${DATASET}.${stagingId}\` st,
                   UNNEST(st.career) AS c_item
            ) S
            ON T.horse_id = S.horse_id
               AND T.race_year = S.race_year
               AND T.race_type = S.race_type
            WHEN MATCHED THEN
              UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT(
                horse_id, race_year, race_type, horse_age, race_count,
                race_won_count, race_prize_count, prize_amounts, prize_currencies,
                created_date, last_updated_date
              ) VALUES (
                S.horse_id, S.race_year, S.race_type, S.horse_age, S.race_count,
                S.race_won_count, S.race_prize_count, S.prize_amounts, S.prize_currencies,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
          `;
          console.debug('↗️ MERGE into HORSE_CAREERS');
          const [job2] = await bigquery.createQueryJob({ query: sql });
          await job2.getQueryResults();
          const stats2 = job2.metadata.statistics.dmlStats || {};
          console.info(`✅ HORSE_CAREERS MERGE: inserted=${stats2.insertedRowCount||0}, updated=${stats2.updatedRowCount||0}`);
        }

        // MERGE RACES
        {
          const sql = `
            MERGE \`${DATASET}.RACES\` T
            USING (
              SELECT DISTINCT
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.race_id') AS INT64) AS race_id,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.race_number') AS INT64) AS race_number,
                JSON_EXTRACT_SCALAR(r_item, '$.race_name') AS race_name,
                TIMESTAMP_MILLIS(CAST(JSON_EXTRACT_SCALAR(r_item, '$.race_date') AS INT64)) AS race_date,
                JSON_EXTRACT_SCALAR(r_item, '$.race_currency') AS currency_code,
                JSON_EXTRACT_SCALAR(r_item, '$.currency_symbol') AS currency_symbol,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.duration_ms') AS INT64) AS duration_ms,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.track_distance_m') AS INT64) AS track_distance_m,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.temperature_c') AS FLOAT64) AS temperature_c,
                JSON_EXTRACT_SCALAR(r_item, '$.weather') AS weather,
                JSON_EXTRACT_SCALAR(r_item, '$.race_group') AS race_group,
                JSON_EXTRACT_SCALAR(r_item, '$.subtype') AS subtype,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.category_id') AS INT64) AS category_id,
                JSON_EXTRACT_SCALAR(r_item, '$.category_breed') AS category_breed,
                JSON_EXTRACT_SCALAR(r_item, '$.category_name') AS category_name,
                JSON_EXTRACT_SCALAR(r_item, '$.country_code') AS country_code,
                JSON_EXTRACT_SCALAR(r_item, '$.city_name') AS city_name,
                JSON_EXTRACT_SCALAR(r_item, '$.track_type') AS track_type,
                JSON_EXTRACT_SCALAR(r_item, '$.video_url') AS video_url,
                JSON_EXTRACT_SCALAR(r_item, '$.race_rules') AS race_rules,
                JSON_EXTRACT_SCALAR(r_item, '$.payments') AS payments,
                JSON_EXTRACT_SCALAR(r_item, '$.race_style') AS race_style
              FROM \`${DATASET}.${stagingId}\` st,
                   UNNEST(st.races) AS r_item
            ) S
            ON T.race_id = S.race_id
            WHEN MATCHED THEN
              UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT(
                race_id, race_number, race_name, race_date,
                currency_code, currency_symbol, duration_ms,
                track_distance_m, temperature_c, weather,
                race_group, subtype, category_id, category_breed,
                category_name, country_code, city_name,
                track_type, video_url, race_rules, payments,
                race_style, created_date, last_updated_date
              ) VALUES (
                S.race_id, S.race_number, S.race_name, S.race_date,
                S.currency_code, S.currency_symbol, S.duration_ms,
                S.track_distance_m, S.temperature_c, S.weather,
                S.race_group, S.subtype, S.category_id, S.category_breed,
                S.category_name, S.country_code, S.city_name,
                S.track_type, S.video_url, S.race_rules, S.payments,
                S.race_style, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
          `;
          console.debug('↗️ MERGE into RACES');
          const [job3] = await bigquery.createQueryJob({ query: sql });
          await job3.getQueryResults();
          const stats3 = job3.metadata.statistics.dmlStats || {};
          console.info(`✅ RACES MERGE: inserted=${stats3.insertedRowCount||0}, updated=${stats3.updatedRowCount||0}`);
        }

        // MERGE RACE_RECORDS
        {
          const sql = `
            MERGE \`${DATASET}.RACE_RECORDS\` T
            USING (
              SELECT
                FARM_FINGERPRINT(
                  CONCAT(
                    CAST(j_item AS STRING), '_',
                    CAST(JSON_EXTRACT_SCALAR(r_item, '$.race_id') AS STRING), '_',
                    JSON_EXTRACT_SCALAR(r_item, '$.start_order')
                  )
                ) AS race_record_id,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.race_id') AS INT64) AS race_id,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.horse_id') AS INT64) AS horse_id,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.start_order') AS INT64) AS start_order,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.finish_place') AS INT64) AS finish_place,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.jockey_weight_kg') AS FLOAT64) AS jockey_weight_kg,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.prize_amount') AS FLOAT64) AS prize_amount,
                JSON_EXTRACT_SCALAR(r_item, '$.prize_currency') AS prize_currency,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.jockey_id') AS INT64) AS jockey_id,
                CAST(JSON_EXTRACT_SCALAR(r_item, '$.trainer_id') AS INT64) AS trainer_id
              FROM \`${DATASET}.${stagingId}\` st,
                   UNNEST(st.races) AS j_item,
                   UNNEST([j_item]) AS r_item
            ) S
            ON T.race_record_id = S.race_record_id
            WHEN MATCHED THEN
              UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT(
                race_record_id, race_id, horse_id, start_order, finish_place,
                jockey_weight_kg, prize_amount, prize_currency, jockey_id,
                trainer_id, created_date, last_updated_date
              ) VALUES (
                S.race_record_id, S.race_id, S.horse_id, S.start_order, S.finish_place,
                S.jockey_weight_kg, S.prize_amount, S.prize_currency, S.jockey_id,
                S.trainer_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
          `;
          console.debug('↗️ MERGE into RACE_RECORDS');
          const [job4] = await bigquery.createQueryJob({ query: sql });
          await job4.getQueryResults();
          const stats4 = job4.metadata.statistics.dmlStats || {};
          console.info(`✅ RACE_RECORDS MERGE: inserted=${stats4.insertedRowCount||0}, updated=${stats4.updatedRowCount||0}`);
        }

        // Cleanup raw staging
        console.debug(`🗑️ Dropping raw staging ${stagingTable}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingTable}\`` });
        console.info(`✅ Dropped raw staging ${stagingTable}`);
      }
    } else {
      // Reference tables logic unchanged...
      const refTables = prefixToTables[prefix] || [];
      for (const table of refTables) {
        // ... existing staging+merge
      }
    }

    // 4. Update watermark
    const newest = newFiles.map(e => e.created).sort().pop();
    await bigquery.query({
      query: `
        MERGE \`${METADATA_TABLE}\` M
        USING (
          SELECT @prefix AS prefix, @ts AS last_processed_time
        ) N
        ON M.prefix = N.prefix
        WHEN MATCHED THEN
          UPDATE SET last_processed_time = N.last_processed_time
        WHEN NOT MATCHED THEN
          INSERT(prefix, last_processed_time)
          VALUES(N.prefix, N.last_processed_time)
      `,
      params: { prefix, ts: newest }
    });
    console.info('✅ Updated metadata');

    return res.status(200).json({ prefix, processedFiles: newFiles.length, lastProcessed: newest });

  } catch (err) {
    console.error('❌ ingest error:', err);
    return res.status(500).send(`ingest failed: ${err.message}`);
  }
};
