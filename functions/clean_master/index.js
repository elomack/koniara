// index.js for cleanMaster Cloud Function
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const BUCKET = process.env.BUCKET_NAME; // e.g. 'horse-predictor-v2-data'

/**
 * HTTP trigger parameters:
 *   prefix (string) – folder containing merged master files to process, e.g. 'horse_data/'. Required.
 *
 * This function scans for MASTERFILE_*.ndjson under the given prefix,
 * dedupes and strips malformed lines, and writes CLEANED_ files.
 * Returns cleanedUri and createdTime for each file for ingestion metadata.
 */
exports.cleanMaster = async (req, res) => {
  console.debug('ℹ️ cleanMaster invoked with body:', req.body);
  try {
    const { prefix } = req.body;
    if (!prefix || typeof prefix !== 'string') {
      console.warn('❗ Missing or invalid required field: prefix');
      return res.status(400).send('⚠️ Missing or invalid required field: prefix');
    }
    const bucket = storage.bucket(BUCKET);

    // 1. List all files under specified prefix
    console.debug(`📋 Listing files under prefix: ${prefix}`);
    const [files] = await bucket.getFiles({ prefix });

    // 2. Filter for master NDJSON files (exclude CLEANED_ files)
    const masters = files
      .map(f => f.name)
      .filter(name => name.startsWith(prefix + 'MASTERFILE_') && name.endsWith('.ndjson'));
    console.debug(`🔍 Found ${masters.length} master files under ${prefix}`);

    const processed = [];
    for (const masterPath of masters) {
      const fileName = masterPath.substring(prefix.length);
      const cleanedName = `${prefix}CLEANED_${fileName}`;

      const cleanedFile = bucket.file(cleanedName);
      const [exists] = await cleanedFile.exists();
      if (exists) {
        console.debug(`⚠️ Skipping already-cleaned file: ${cleanedName}`);
        continue;
      }

      console.debug(`➡️ Processing master file: ${masterPath}`);

      // Streams for reading and writing
      const masterFile = bucket.file(masterPath);
      const readStream = masterFile.createReadStream();
      const writeStream = cleanedFile.createWriteStream({ contentType: 'application/x-ndjson' });

      let initialCount = 0;
      let removedCount = 0;
      const seen = new Set();
      const rl = require('readline').createInterface({ input: readStream });

      // Read, parse, dedupe
      for await (const line of rl) {
        initialCount++;
        let obj;
        try {
          obj = JSON.parse(line);
        } catch (err) {
          console.warn(`⚠️ Malformed JSON on line ${initialCount}, skipping`);
          removedCount++;
          continue;
        }
        const repr = JSON.stringify(obj);
        if (seen.has(repr)) {
          removedCount++;
          continue;
        }
        seen.add(repr);
        writeStream.write(repr + '\n');
      }

      // Finalize write stream
      await new Promise((ok, ko) => {
        writeStream.end(() => {
          console.debug('✋ Finished writing cleaned file');
          ok();
        });
        writeStream.on('error', err => ko(err));
      });

      // Retrieve URI and creation time for metadata
      const [metadata] = await cleanedFile.getMetadata();
      const cleanedUri = `gs://${BUCKET}/${cleanedName}`;
      const createdTime = metadata.timeCreated;

      // Collect stats including new URI and timestamp
      processed.push({
        prefix,
        masterFile: masterPath,
        cleanedFile: cleanedName,
        cleanedUri,
        createdTime,
        initialCount,
        removedCount,
        finalCount: initialCount - removedCount
      });

      console.debug(`✅ Cleaned: ${cleanedName} | Cleaned URI: ${cleanedUri} |Initial: ${initialCount} | Removed: ${removedCount} | Final: ${initialCount - removedCount} | Created: ${createdTime}`);
    }

    if (processed.length === 0) {
      console.warn(`⚠️ No fresh master files found under ${prefix}`);
      return res.status(204).send(`No fresh master files to clean under ${prefix}`);
    }

    // Return stats for newly cleaned files including prefix for workflow chaining
    return res.status(200).json({ prefix, processed });

  } catch (err) {
    console.error('❌ cleanMaster error:', err);
    return res.status(500).send(`cleanMaster failed: ${err.message}`);
  }
};
