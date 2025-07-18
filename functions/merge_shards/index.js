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
    console.debug('✨ mergeShards invoked with body:', req.body);
    const { prefix, outputPrefix, pattern } = req.body;
    if (!prefix || !pattern || !outputPrefix) {
      console.warn('❗ Missing required fields: prefix, pattern, outputPrefix');
      return res.status(400).send('⚠️ Missing required fields: prefix, pattern, outputPrefix');
    }

    const bucket = storage.bucket(BUCKET);
    const regex = new RegExp(pattern);

    // 1. List all files under prefix
    console.debug(`📋 Listing files under prefix: ${prefix}`);
    const [files] = await bucket.getFiles({ prefix });
    const shardNames = files
      .map(f => f.name.substring(prefix.length))
      .filter(name => regex.test(name))
      .sort();

    console.debug(`🔍 Found ${shardNames.length} shards matching pattern`);

    // Handle no shards gracefully
    if (shardNames.length === 0) {
      console.info(`ℹ️ No shards found under ${prefix} matching ${pattern}`);
      return res.status(200).json({ mergedCount: 0, masterFile: null, message: 'No shards to merge' });
    }

    // 2. Build master filename dynamically based on outputPrefix
    const ts = new Date().toISOString().replace(/[:.]/g, '_');
    // derive a clean uppercase tag from outputPrefix: remove trailing slash and underscores
    const folderKey = outputPrefix.replace(/\/$/, '');
    const tag = folderKey.replace(/_/g, '').toUpperCase();
    const masterName = `${outputPrefix}MASTERFILE_${tag}_${ts}.ndjson`;
    const masterFile = bucket.file(masterName);
    console.debug(`🖊️ Creating master file at ${masterName}`);

    // 3. Stream‐concatenate
    const writeStream = masterFile.createWriteStream({ contentType: 'application/x-ndjson' });
    for (const name of shardNames) {
      console.debug(`➡️ Appending shard: ${name}`);
      await new Promise((ok, ko) => {
        bucket.file(prefix + name)
          .createReadStream()
          .on('error', err => {
            console.error(`❌ Error reading shard ${name}:`, err);
            ko(err);
          })
          .on('end', () => {
            console.debug(`✅ Finished appending ${name}`);
            ok();
          })
          .pipe(writeStream, { end: false });
      });
      writeStream.write('\n');
    }
    await new Promise((ok, ko) => {
      writeStream.end(() => {
        console.debug('✋ Finished writing master file');
        ok();
      });
      writeStream.on('error', err => {
        console.error('❌ Error finalizing master file:', err);
        ko(err);
      });
    });

    // 4. Delete shards (commented out until prod)
    // console.debug('🗑️ Deleting shard files');
    // await Promise.all(shardNames.map(name =>
    //   bucket.file(prefix + name)
    //     .delete()
    //     .then(() => console.debug(`🗑️ Deleted ${name}`))
    //     .catch(err => console.warn(`⚠️ Failed to delete ${name}:`, err))
    // ));

    console.info(`🎉 mergeShards completed. Master: ${masterName}, Count: ${shardNames.length}`);
    return res.status(200).json({ masterFile: masterName, mergedCount: shardNames.length });
  } catch (err) {
    console.error('❌ mergeShards error:', err);
    return res.status(500).send(`❌ mergeShards failed: ${err.message}`);
  }
};
