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

    // 3. Branch by prefix and process each file
    if (prefix === 'horse_data/') {
      for (const entry of newFiles) {
        // derive staging table tag from filename timestamp
        const name = entry.file.name;
        const m = name.match(/CLEANED_MASTERFILE_[^_]+_([0-9]{8}T[0-9_]+Z)\.ndjson$/);
        const tag = m ? m[1] : entry.created.toISOString().replace(/[:.-]/g,'_');
        const stagingId = `raw_horse_data_${tag}`;
        const uri = `gs://${BUCKET}/${name}`;

        // Load raw JSON into staging table
        console.debug(`⬆️ Loading raw JSON into staging table ${stagingId}`);
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
        console.info(`✅ Loaded raw staging table ${stagingId}`);

        // MERGE logic for HORSES table (step 1)
        {
          const stagingTableFull = `${DATASET}.${stagingId}`;

          // 1. Compute staging distinct and existing counts for manual insert count
          const [stageCountRows] = await bigquery.query({
            query: `SELECT COUNT(DISTINCT horse_id) AS cnt FROM \`${stagingTableFull}\``
          });
          const stagingCount = stageCountRows[0].cnt || 0;

          const [existingCountRows] = await bigquery.query({
            query: `SELECT COUNT(DISTINCT S.horse_id) AS cnt
                    FROM \`${DATASET}.HORSES\` T
                    JOIN (
                      SELECT DISTINCT horse_id
                      FROM \`${stagingTableFull}\`
                    ) S
                    ON T.horse_id = S.horse_id`
          });
          const existingCount = existingCountRows[0].cnt || 0;
          const manualInserted = stagingCount - existingCount;

          // 2. Perform MERGE
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
              FROM \`${stagingTableFull}\`
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

          // Log manual insert count and no DML stats available
          console.info(`✅ HORSES MERGE: inserted=${manualInserted}, updated=${existingCount}`);
        }

        // MERGE logic for HORSE_CAREERS table (step 2)
        {
          const stagingTableFull = `${DATASET}.${stagingId}`;

          // 1. Compute staging and existing counts for manual insert count
          const [stageRowsHC] = await bigquery.query({
            query: `SELECT COUNT(*) AS cnt
                    FROM \`${stagingTableFull}\`,
                         UNNEST(career) AS c_item`
          });
          const stagingCountHC = stageRowsHC[0].cnt || 0;

          const [existingRowsHC] = await bigquery.query({
            query: `SELECT COUNT(*) AS cnt
                    FROM (
                      SELECT DISTINCT
                        CAST(st.horse_id AS INT64) AS horse_id,
                        c_item.race_year           AS race_year,
                        c_item.race_type           AS race_type
                      FROM \`${stagingTableFull}\` AS st,
                           UNNEST(st.career) AS c_item
                    ) S
                    JOIN \`${DATASET}.HORSE_CAREERS\` AS T
                      ON S.horse_id = T.horse_id
                     AND S.race_year = T.race_year
                     AND S.race_type = T.race_type`
          });
          const existingCountHC = existingRowsHC[0].cnt || 0;
          const manualInsertedHC = stagingCountHC - existingCountHC;

          // 2. Perform MERGE
          const sqlHC = `
            MERGE \`${DATASET}.HORSE_CAREERS\` T
            USING (
              SELECT
                CAST(st.horse_id AS INT64)         AS horse_id,
                c_item.race_year                   AS race_year,
                c_item.race_type                   AS race_type,
                c_item.horse_age                   AS horse_age,
                c_item.race_count                  AS race_count,
                c_item.race_won_count              AS race_won_count,
                c_item.race_prize_count            AS race_prize_count,
                CAST(c_item.prize_amounts AS STRING)    AS prize_amounts,
                CAST(c_item.prize_currencies AS STRING) AS prize_currencies
              FROM \`${stagingTableFull}\` AS st,
                   UNNEST(st.career) AS c_item
            ) S
            ON T.horse_id = S.horse_id
               AND T.race_year = S.race_year
               AND T.race_type = S.race_type
            WHEN MATCHED THEN
              UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT(
                horse_id, race_year, race_type,
                horse_age, race_count, race_won_count, race_prize_count,
                prize_amounts, prize_currencies,
                created_date, last_updated_date
              ) VALUES (
                S.horse_id, S.race_year, S.race_type,
                S.horse_age, S.race_count, S.race_won_count, S.race_prize_count,
                S.prize_amounts, S.prize_currencies,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
          `;
          console.debug('↗️ MERGE into HORSE_CAREERS');
          const [jobHC] = await bigquery.createQueryJob({ query: sqlHC });
          await jobHC.getQueryResults();

          // Log manual insert count
          console.debug(`✅ HORSE_CAREERS MERGE: inserted=${manualInsertedHC}, updated=${existingCountHC}`);


        }
        // MERGE logic for RACES table (step 3)
        {
          // 1. Compute staging distinct and existing counts for manual insert count
          const stagingTableFull = `${DATASET}.${stagingId}`;
          // Count distinct races in staging
          const [stageCountRowsR] = await bigquery.query({
            query: `
              SELECT COUNT(DISTINCT CAST(race_id AS INT64)) AS cnt
              FROM \`${stagingTableFull}\`, UNNEST(races) AS r
            `
          });
          const stagingCountR = stageCountRowsR[0].cnt || 0;
          // Count how many of those races already exist in RACES
          const [existingCountRowsR] = await bigquery.query({
            query: `
              SELECT COUNT(*) AS cnt
              FROM (
                SELECT DISTINCT CAST(r_item.race_id AS INT64) AS race_id
                FROM \`${stagingTableFull}\`, UNNEST(races) AS r_item
              ) S
              JOIN \`${DATASET}.RACES\` T
              ON S.race_id = T.race_id
            `
          });
          const existingCountR = existingCountRowsR[0].cnt || 0;
          const manualInsertedR = stagingCountR - existingCountR;

          // 2. Build and run MERGE for RACES
          const sqlR = `
            MERGE \`${DATASET}.RACES\` T
            USING (
              SELECT DISTINCT
                CAST(r_item.race_id AS INT64)                    AS race_id,
                CAST(r_item.race_number AS INT64)                AS race_number,
                r_item.race_name                                  AS race_name,
                TIMESTAMP_MILLIS(r_item.race_date)                AS race_date,
                r_item.prize_currency                             AS currency_code,
                CAST(NULL AS STRING)                              AS currency_symbol,
                CAST(NULL AS INT64)                               AS duration_ms,
                CAST(r_item.track_distance_m AS INT64)            AS track_distance_m,
                CAST(r_item.temperature_c AS FLOAT64)             AS temperature_c,
                r_item.weather                                    AS weather,
                r_item.race_group                                 AS race_group,
                r_item.subtype                                    AS subtype,
                CAST(r_item.category_id AS INT64)                 AS category_id,
                r_item.category_breed                             AS category_breed,
                r_item.category_name                              AS category_name,
                r_item.country_code                               AS country_code,
                r_item.city_name                                  AS city_name,
                r_item.track_type                                 AS track_type,
                r_item.video_url                                  AS video_url,
                r_item.race_rules                                 AS race_rules,
                r_item.payments                                   AS payments,
                r_item.race_style                                 AS race_style
              FROM \`${stagingTableFull}\` AS st,
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
              )
              VALUES (
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
          const [jobR] = await bigquery.createQueryJob({ query: sqlR });
          await jobR.getQueryResults();
          // Retrieve and log DML stats for RACES
          const [metaR] = await jobR.getMetadata();
          const statsR = metaR.statistics?.dmlStats || {};
          console.debug(`✅ RACES MERGE: inserted=${manualInsertedR}, updated=${existingCountR}`);

        }
        // MERGE logic for RACE_RECORDS table (step 4)
        {
          const stagingTableFull = `${DATASET}.${stagingId}`;

          // 1. Compute staging and existing counts for manual insert count
          const [stageCountRowsRR] = await bigquery.query({
            query: `
              SELECT COUNT(*) AS cnt
              FROM \`${stagingTableFull}\` AS st,
              UNNEST(st.races)       AS r_item
            `
          });
          const stagingCountRR = stageCountRowsRR[0].cnt || 0;

          console.debug(`staging_count done = ${stagingCountRR}`);

          const [existingCountRowsRR] = await bigquery.query({
            query: `
              WITH staging_data AS (
                SELECT DISTINCT
                  FARM_FINGERPRINT(
                    CONCAT(
                      CAST(st.horse_id AS STRING),'_',CAST(r_item.race_id AS STRING),'_',CAST(r_item.start_order AS STRING)
                    )
                  ) AS race_record_id
                FROM \`${stagingTableFull}\` AS st,
                     UNNEST(st.races) AS r_item
              )
              SELECT COUNT(*) AS cnt
              FROM staging_data AS S
              JOIN \`${DATASET}.RACE_RECORDS\` AS T
              ON S.race_record_id = T.race_record_id
            `
          });
          const existingCountRR = existingCountRowsRR[0].cnt || 0;
          const manualInsertedRR = stagingCountRR - existingCountRR;
          console.debug(`existing_count done = ${existingCountRR}`);

          // 2. Perform MERGE for RACE_RECORDS
          const sqlRR = `
            MERGE \`${DATASET}.RACE_RECORDS\` T
            USING (
              SELECT
                FARM_FINGERPRINT(
                  CONCAT(
                    CAST(st.horse_id AS STRING),'_',CAST(r_item.race_id AS STRING),'_',CAST(r_item.start_order AS STRING)
                  )
                ) AS race_record_id,
                r_item.race_id           AS race_id,
                st.horse_id              AS horse_id,
                r_item.start_order       AS start_order,
                SAFE_CAST(r_item.finish_place AS INT64) AS finish_place,
                r_item.jockey_weight_kg  AS jockey_weight_kg,
                r_item.prize_amount      AS prize_amount,
                r_item.prize_currency    AS prize_currency,
                r_item.jockey_id         AS jockey_id,
                r_item.trainer_id        AS trainer_id
              FROM \`${stagingTableFull}\` AS st,
                   UNNEST(st.races) AS r_item
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
          const [jobRR] = await bigquery.createQueryJob({ query: sqlRR });
          await jobRR.getQueryResults();
          console.info(`✅ RACE_RECORDS MERGE: inserted=${manualInsertedRR}, updated=${existingCountRR}`);
        }
        // Cleanup staging table for horse_data
        const stagingTableFull = `${DATASET}.${stagingId}`;
        console.debug(`🗑️ Dropping staging table ${stagingTableFull}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingTableFull}\`` });
        console.info(`✅ Dropped staging table ${stagingTableFull}`);
        }
    } else if (prefix === 'jockey_data/') {
      // Process JOCKEYS reference table
      for (const entry of newFiles) {
        // derive staging table tag
        const name = entry.file.name;
        const m = name.match(/CLEANED_MASTERFILE_[^_]+_([0-9]{8}T[0-9_]+Z)\.ndjson$/);
        const tag = m ? m[1] : entry.created.toISOString().replace(/[:.-]/g,'_');
        const stagingId = `raw_jockey_data_${tag}`;
        stagingTableFull = `${DATASET}.${stagingId}`;
        const uri = `gs://${BUCKET}/${name}`;

        // Load raw JSON into staging
        console.debug(`⬆️ Loading raw JSON into staging table ${stagingId}`);
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
        console.info(`✅ Loaded raw staging table ${stagingId}`);

        // MERGE logic for JOCKEYS
        // 1. Compute staging distinct and existing counts for manual insert count
        const [stageCountRowsJ] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT jockey_id) AS cnt
            FROM \`${stagingTableFull}\`
          `
        });
        const stagingCountJ = stageCountRowsJ[0].cnt || 0;

        const [existingCountRowsJ] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT S.jockey_id) AS cnt
            FROM (
              SELECT DISTINCT CAST(jockey_id AS INT64) AS jockey_id
              FROM \`${stagingTableFull}\`
            ) S
            JOIN \`${DATASET}.JOCKEYS\` T
            ON S.jockey_id = T.jockey_id
          `
        });
        const existingCountJ = existingCountRowsJ[0].cnt || 0;
        const manualInsertedJ = stagingCountJ - existingCountJ;

        // 2. Perform MERGE
        const sqlJ = `
          MERGE \`${DATASET}.JOCKEYS\` T
          USING (
            SELECT
              CAST(jockey_id AS INT64)      AS jockey_id,
              first_name,
              last_name,
              licence_country
            FROM \`${stagingTableFull}\`
          ) S
          ON T.jockey_id = S.jockey_id
          WHEN MATCHED THEN
            UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN
            INSERT(
              jockey_id, first_name, last_name, licence_country,
              created_date, last_updated_date
            ) VALUES (
              S.jockey_id, S.first_name, S.last_name, S.licence_country,
              CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        `;
        console.debug('↗️ MERGE into JOCKEYS');
        const [jobJ] = await bigquery.createQueryJob({ query: sqlJ });
        await jobJ.getQueryResults();

        // Log manual insert count for JOCKEYS
        console.info(`✅ JOCKEYS MERGE: inserted=${manualInsertedJ}, updated=${existingCountJ}`);
        // Cleanup staging table for JOCKEYS
        const stagingTableFull = `${DATASET}.${stagingId}`;
        console.debug(`🗑️ Dropping staging table ${stagingTableFull}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingTableFull}\`` });
        console.info(`✅ Dropped staging table ${stagingTableFull}`);
      }
    } else if (prefix === 'trainer_data/') {
      // Process TRAINERS reference table
      for (const entry of newFiles) {
        // derive staging table tag
        const name = entry.file.name;
        const m = name.match(/CLEANED_MASTERFILE_[^_]+_([0-9]{8}T[0-9_]+Z)\.ndjson$/);
        const tag = m ? m[1] : entry.created.toISOString().replace(/[:.-]/g,'_');
        const stagingId = `raw_trainer_data_${tag}`;
        stagingTableFull = `${DATASET}.${stagingId}`;
        const uri = `gs://${BUCKET}/${name}`;

        // Load raw JSON into staging
        console.debug(`⬆️ Loading raw JSON into staging table ${stagingId}`);
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
        console.info(`✅ Loaded raw staging table ${stagingId}`);

        // MERGE logic for TRAINERS
        // 1. Compute staging distinct and existing counts for manual insert count
        const [stageCountRowsT] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT trainer_id) AS cnt
            FROM \`${stagingTableFull}\`
          `
        });
        const stagingCountT = stageCountRowsT[0].cnt || 0;

        const [existingCountRowsT] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT S.trainer_id) AS cnt
            FROM (
              SELECT DISTINCT CAST(trainer_id AS INT64) AS trainer_id
              FROM \`${stagingTableFull}\`
            ) S
            JOIN \`${DATASET}.TRAINERS\` T
            ON S.trainer_id = T.trainer_id
          `
        });
        const existingCountT = existingCountRowsT[0].cnt || 0;
        const manualInsertedT = stagingCountT - existingCountT;

        // 2. Perform MERGE
        const sqlT = `
          MERGE \`${DATASET}.TRAINERS\` T
          USING (
            SELECT
              CAST(trainer_id AS INT64)      AS trainer_id,
              first_name,
              last_name,
              licence_country
            FROM \`${stagingTableFull}\`
          ) S
          ON T.trainer_id = S.trainer_id
          WHEN MATCHED THEN
            UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN
            INSERT(
              trainer_id, first_name, last_name, licence_country,
              created_date, last_updated_date
            ) VALUES (
              S.trainer_id, S.first_name, S.last_name, S.licence_country,
              CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        `;
        console.debug('↗️ MERGE into TRAINERS');
        const [jobT] = await bigquery.createQueryJob({ query: sqlT });
        await jobT.getQueryResults();

        // Log manual insert count for TRAINERS
        console.info(`✅ TRAINERS MERGE: inserted=${manualInsertedT}, updated=${existingCountT}`);
        // Cleanup staging table for TRAINERS
        const stagingTableFull = `${DATAET}.${stagingId}`;
        console.debug(`🗑️ Dropping staging table ${stagingTableFull}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingTableFull}\`` });
        console.info(`✅ Dropped staging table ${stagingTableFull}`);

      }
    } else if (prefix === 'breeder_data/') {
      // Process BREEDERS reference table
      for (const entry of newFiles) {
        // derive staging table tag
        const name = entry.file.name;
        const m = name.match(/CLEANED_MASTERFILE_[^_]+_([0-9]{8}T[0-9_]+Z)\.ndjson$/);
        const tag = m ? m[1] : entry.created.toISOString().replace(/[:.-]/g,'_');
        const stagingId = `raw_breeder_data_${tag}`;
        stagingTableFull = `${DATASET}.${stagingId}`;
        const uri = `gs://${BUCKET}/${name}`;

        // Load raw JSON into staging
        console.debug(`⬆️ Loading raw JSON into staging table ${stagingId}`);
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
        console.info(`✅ Loaded raw staging table ${stagingId}`);

        // MERGE logic for BREEDERS
        // 1. Compute staging distinct and existing counts for manual insert count
        const [stageCountRowsB] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT breeder_id) AS cnt
            FROM \`${stagingTableFull}\`
          `
        });
        const stagingCountB = stageCountRowsB[0].cnt || 0;

        const [existingCountRowsB] = await bigquery.query({
          query: `
            SELECT COUNT(DISTINCT S.breeder_id) AS cnt
            FROM (
              SELECT DISTINCT CAST(breeder_id AS INT64) AS breeder_id
              FROM \`${stagingTableFull}\`
            ) S
            JOIN \`${DATASET}.BREEDERS\` T
            ON S.breeder_id = T.breeder_id
          `
        });
        const existingCountB = existingCountRowsB[0].cnt || 0;
        const manualInsertedB = stagingCountB - existingCountB;

        // 2. Perform MERGE
        const sqlB = `
          MERGE \`${DATASET}.BREEDERS\` T
          USING (
            SELECT
              CAST(breeder_id AS INT64) AS breeder_id,
              name,
              city
            FROM \`${stagingTableFull}\`
          ) S
          ON T.breeder_id = S.breeder_id
          WHEN MATCHED THEN
            UPDATE SET last_updated_date = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN
            INSERT(
              breeder_id, name, city, created_date, last_updated_date
            ) VALUES (
              S.breeder_id, S.name, S.city,
              CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        `;
        console.debug('↗️ MERGE into BREEDERS');
        const [jobB] = await bigquery.createQueryJob({ query: sqlB });
        await jobB.getQueryResults();

        // Log manual insert count for BREEDERS
        console.info(`✅ BREEDERS MERGE: inserted=${manualInsertedB}, updated=${existingCountB}`);
        // Cleanup staging table for BREEDERS
        const stagingTableFull = `${DATASET}.${stagingId}`;
        console.debug(`🗑️ Dropping staging table ${stagingTableFull}`);
        await bigquery.query({ query: `DROP TABLE \`${stagingTableFull}\`` });
        console.info(`✅ Dropped staging table ${stagingTableFull}`);
      }
    } else {
      console.debug(`❌ input error: expected prefix horse, breeder, trainer or jocket`);
    }

    // 4. Update watermark
    console.debug('🔄 Updating watermark');
    // Compute the newest processed timestamp
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

    return res.status(200).json({ prefix, processedFiles: newFiles.length });

  } catch (err) {
    console.error('❌ ingest error:', err);
    return res.status(500).send(`ingest failed: ${err.message}`);
  }
};
