// index.js for ingest Cloud Function
// This function loads new cleaned NDJSON files into BigQuery staging tables
// and merges them into production tables, updating ingestion_metadata.

const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');

// Initialize clients
const storage = new Storage();
const bigquery = new BigQuery();

// Environment variables
const BUCKET = process.env.BUCKET_NAME;            // e.g. 'horse-predictor-v2-data'
const DATASET = process.env.BQ_DATASET;            // e.g. 'horse_data_v2'
const METADATA_TABLE = `${bigquery.projectId}.${DATASET}.ingestion_metadata`;

// Map each prefix to its target tables
const prefixToTables = {
  'breeder_data/': ['BREEDERS'],
  'jockey_data/': ['JOCKEYS'],
  'trainer_data/': ['TRAINERS'],
  'horse_data/': ['HORSES','HORSE_CAREERS','RACES','RACE_RECORDS'],
};

exports.ingest = async (req, res) => {
  console.debug('ℹ️ ingest invoked with body:', req.body);
  try {
    const { prefix, processed } = req.body;

    // 1. Validate input
    if (!prefix || typeof prefix !== 'string') {
      console.warn('❗ Missing or invalid required field: prefix');
      return res.status(400).send('⚠️ Missing or invalid required field: prefix');
    }
    if (!processed || !Array.isArray(processed) || processed.length === 0) {
      console.info(`⚠️ No cleaned files provided for prefix ${prefix}`);
      return res.status(204).send(`No new files to ingest for ${prefix}`);
    }

    // 2. Fetch last processed timestamp from metadata table
    const [metaRows] = await bigquery.query({
      query: `SELECT last_processed_time FROM ${METADATA_TABLE} WHERE prefix = @prefix`,
      params: { prefix }
    });
    let lastProcessedTime = metaRows.length ? metaRows[0].last_processed_time : '1970-01-01T00:00:00Z';
    console.debug(`Last processed time for ${prefix}:`, lastProcessedTime);

    // 3. Filter processed entries for new files only
    const newEntries = processed.filter(e => new Date(e.createdTime) > new Date(lastProcessedTime));
    if (newEntries.length === 0) {
      console.info(`⚠️ No new cleaned files since ${lastProcessedTime} for ${prefix}`);
      return res.status(204).send(`No new files to ingest for ${prefix}`);
    }

    // 4. For each target table: load and merge
    for (const table of prefixToTables[prefix] || []) {
      // Derive staging table partition suffix YYYYMMDD from today (or use max createdTime date)
      const partitionDate = new Date().toISOString().slice(0,10).replace(/-/g,'');
      const stagingTable = `${DATASET}.stg_${table}$${partitionDate}`;
      const uris = newEntries.map(e => e.cleanedUri);

      console.debug(`⬆️ Loading into staging ${stagingTable}:`, uris);
      // 4a. Load job
      const [job] = await bigquery
        .dataset(DATASET)
        .table(`stg_${table}$${partitionDate}`)
        .load(uris, {
          sourceFormat: 'NEWLINE_DELIMITED_JSON',
          autodetect: false,        // or specify explicit schema
          writeDisposition: 'WRITE_TRUNCATE'
        });
      await job.promise();
      console.info(`✅ Loaded ${uris.length} files into ${stagingTable}`);

      // 4b. Merge into production table
      const prodTable = `${DATASET}.${table}`;
      const mergeSql = `
        MERGE \`${bigquery.projectId}.${prodTable}\` T
        USING \`${bigquery.projectId}.${stagingTable}\` S
        ON /* match condition depending on table, e.g. */ T.${table.toLowerCase()}_id = S.${table.toLowerCase()}_id
        WHEN MATCHED THEN UPDATE SET /* field mappings */
        WHEN NOT MATCHED THEN INSERT ROW
      `;
      console.debug(`↗️ Merging staging into ${prodTable}`);
      await bigquery.query({ query: mergeSql });
      console.info(`✅ Merged into ${prodTable}`);
    }

    // 5. Update metadata table with max createdTime
    const maxTime = newEntries
      .map(e => new Date(e.createdTime))
      .reduce((a,b) => a > b ? a : b)
      .toISOString();
    console.debug(`Updating metadata for ${prefix} to ${maxTime}`);
    const upsertSql = `
      MERGE \`${METADATA_TABLE}\` M
      USING (SELECT @prefix AS prefix, @ts AS last_processed_time) AS N
      ON M.prefix = N.prefix
      WHEN MATCHED THEN UPDATE SET last_processed_time = N.last_processed_time
      WHEN NOT MATCHED THEN INSERT (prefix, last_processed_time) VALUES (N.prefix, N.last_processed_time)
    `;
    await bigquery.query({ query: upsertSql, params: { prefix, ts: maxTime } });
    console.info(`✅ Updated ingestion_metadata for ${prefix}`);

    return res.status(200).json({ prefix, ingestedFiles: newEntries.length, lastProcessedTime: maxTime });

  } catch (err) {
    console.error('❌ ingest error:', err);
    return res.status(500).send(`ingest failed: ${err.message}`);
  }
};
