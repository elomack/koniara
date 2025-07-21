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

        // Placeholder: MERGE logic for HORSE_CAREERS
        // TODO: Add SELECT & MERGE logic for HORSE_CAREERS table (step 2)

        // Placeholder: MERGE logic for RACES
        // TODO: Add SELECT & MERGE logic for RACES table (step 3)

        // Placeholder: MERGE logic for RACE_RECORDS
        // TODO: Add SELECT & MERGE logic for RACE_RECORDS table (step 4)

        // Cleanup staging table
        console.debug(`🗑️ Cleaning up staging table ${stagingId}`);
        // TODO: DROP staging table
      }
    } else {
      // Reference tables logic placeholder
      // TODO: Add processing for breeder, jockey, trainer tables
    }

    // 4. Update watermark
    console.debug('🔄 Updating watermark');
    // TODO: Implement watermark update

    return res.status(200).json({ prefix, processedFiles: newFiles.length });

  } catch (err) {
    console.error('❌ ingest error:', err);
    return res.status(500).send(`ingest failed: ${err.message}`);
  }
};
