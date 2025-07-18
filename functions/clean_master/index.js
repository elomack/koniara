// index.js for cleanMaster Cloud Function
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const BUCKET = process.env.BUCKET_NAME; // e.g. 'horse-predictor-v2-data'

/**
 * HTTP trigger parameters (none required).
 * This function will scan for master files under each scraper prefix,
 * dedupe and strip malformed lines, and write CLEANED_ files.
 */
exports.cleanMaster = async (req, res) => {
  console.debug('ℹ️ cleanMaster invoked');
  try {
    const bucket = storage.bucket(BUCKET);

    // 1. List all master NDJSON files
    console.debug('📋 Listing all files in bucket');
    const [files] = await bucket.getFiles();

    // Filter master NDJSON files created by mergeShards
    const masters = files
      .map(f => f.name)
      .filter(name => /MASTERFILE_.*\.ndjson$/.test(name));

    console.debug(`🔍 Found ${masters.length} master files`);
    let processed = [];

    for (const masterPath of masters) {
      // Derive cleaned filename
      const parts = masterPath.split('/');
      const fileName = parts.pop();
      const dir = parts.join('/') + (parts.length ? '/' : '');
      const cleanedName = `${dir}CLEANED_${fileName}`;

      // Check if already cleaned
      const cleanedFile = bucket.file(cleanedName);
      const [exists] = await cleanedFile.exists();
      if (exists) {
        console.info(`⚠️ Skipping already-cleaned file: ${cleanedName}`);
        continue;
      }

      console.debug(`➡️ Processing master file: ${masterPath}`);
      const masterFile = bucket.file(masterPath);
      const [masterStream] = masterFile;

      // Read file content line-by-line
      const readStream = masterFile.createReadStream();
      const writeStream = cleanedFile.createWriteStream({ contentType: 'application/x-ndjson' });

      let initialCount = 0;
      let removedCount = 0;
      const seen = new Set();
      const rl = require('readline').createInterface({ input: readStream });

      for await (const line of rl) {
        let obj;
        initialCount++;
        // Try parse JSON
        try {
          obj = JSON.parse(line);
        } catch (err) {
          console.warn(`⚠️ Malformed JSON on line ${initialCount}, skipping`);
          removedCount++;
          continue;
        }

        const repr = JSON.stringify(obj);
        // Dedup logic
        if (seen.has(repr)) {
          removedCount++;
          continue;
        }
        seen.add(repr);

        // Write valid & unique line
        writeStream.write(repr + '\n');
      }

      // Finalize write
      await new Promise((ok, ko) => {
        writeStream.end(() => {
          console.debug('✋ Finished writing cleaned file');
          ok();
        });
        writeStream.on('error', err => ko(err));
      });

      processed.push({
        masterFile: masterPath,
        cleanedFile: cleanedName,
        initialCount,
        removedCount,
        finalCount: initialCount - removedCount
      });

      console.info(`✅ Cleaned: ${cleanedName} | Initial: ${initialCount} | Removed: ${removedCount} | Final: ${initialCount - removedCount}`);
    }

    if (processed.length === 0) {
      console.warn('⚠️ No fresh masterfiles found');
      return res.status(204).send('No fresh master files to clean');
    }

    // Return stats for newly cleaned files
    return res.status(200).json({ processed });

  } catch (err) {
    console.error('❌ cleanMaster error:', err);
    return res.status(500).send(`cleanMaster failed: ${err.message}`);
  }
};