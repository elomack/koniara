// index.js
const { Storage } = require('@google-cloud/storage');

const storage = new Storage();
const BUCKET = process.env.BUCKET_NAME; // e.g. 'horse-predictor-v2-data'

/**
 * HTTP body parameters:
 *   prefix       (string) – folder containing shards, e.g. 'horse_data/'
 *   outputPrefix (string) – same as prefix to drop master into
 *   pattern      (string) – regex to match shard filenames, e.g. '^shard_.*\\.ndjson$'
 */
exports.mergeShards = async (req, res) => {
  try {
    const { prefix, outputPrefix, pattern } = req.body;
    if (!prefix || !pattern || !outputPrefix) {
      return res.status(400).send('Missing required fields: prefix, pattern, outputPrefix');
    }
    const bucket = storage.bucket(BUCKET);
    const regex = new RegExp(pattern);

    // 1. List all files under prefix
    const [files] = await bucket.getFiles({ prefix });
    const shardNames = files
      .map(f => f.name.substring(prefix.length))
      .filter(name => regex.test(name))
      .sort();

    if (shardNames.length === 0) {
      return res.status(404).send(`No shards matching ${pattern} under ${prefix}`);
    }

    // 2. Build master filename
    const ts = new Date().toISOString().replace(/[:.]/g,'_');
    const masterName = `${outputPrefix}MASTERFILE_${prefix
      .replace(/\/$/,'').toUpperCase()}_${ts}.ndjson`;
    const masterFile = bucket.file(masterName);

    // 3. Stream‐concatenate
    const writeStream = masterFile.createWriteStream({ contentType: 'application/x-ndjson' });
    for (const name of shardNames) {
      await new Promise((ok, ko) => {
        bucket.file(prefix + name)
          .createReadStream()
          .on('error', ko)
          .on('end', ok)
          .pipe(writeStream, { end: false });
      });
      writeStream.write('\n');
    }
    await new Promise((ok, ko) => writeStream.end(ok).on('error', ko));

    // 4. Delete shards
    await Promise.all(shardNames.map(name => bucket.file(prefix + name).delete()));

    // Done
    res.status(200).json({ masterFile: masterName, mergedCount: shardNames.length });
  } catch (err) {
    console.error('mergeShards error:', err);
    res.status(500).send(err.message);
  }
};
