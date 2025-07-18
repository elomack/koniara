# 🔗 mergeShards Cloud Function

An HTTP-triggered Cloud Function that merges multiple NDJSON shard files from a GCS prefix into a single “master” NDJSON file.

---

## Table of Contents

1. [Overview](#overview)  
2. [Input Parameters](#input-parameters)  
3. [Behavior & Flow](#behavior--flow)  
4. [Prerequisites](#prerequisites)  
5. [Local Development & Testing](#local-development--testing)  
6. [Deploying](#deploying)  
7. [Invoking / Running](#invoking--running)  
8. [Logging & Error Handling](#logging--error-handling)  
9. [Next Steps](#next-steps)  

---

## Overview

`mergeShards`:

- Lists all files under a specified GCS **prefix** (e.g. `horse_data/`)  
- Filters filenames matching a **pattern** (regex for shard naming)  
- Streams & concatenates them—newline by newline—into a single master file  
- Writes the merged file back under an **outputPrefix**, dynamically named  
- Returns JSON `{ masterFile, mergedCount }` or `{ mergedCount: 0, message }` if nothing to merge  

Shard deletion is currently **commented out** until production readiness.

---

## Input Parameters

Send a JSON body with:

| Field          | Type    | Description                                                                   |
| -------------- | ------- | ----------------------------------------------------------------------------- |
| `prefix`       | String  | GCS folder to scan (e.g. `"horse_data/"`)                                      |
| `outputPrefix` | String  | GCS folder for the master file (often same as `prefix`)                        |
| `pattern`      | String  | Regex string to match shard filenames (e.g. `"^shard_.*\\.ndjson$"`)           |

---

## Behavior & Flow

1. **Validate inputs**: Requires `prefix`, `outputPrefix`, `pattern`.  
2. **List shards**: Calls `bucket.getFiles({ prefix })`.  
3. **Filter & sort**: Keeps only names matching `pattern`.  
4. **Early exit**: If no shards, returns `200` with `{ mergedCount: 0, message: 'No shards to merge' }`.  
5. **Build master filename**: Derives `TAG` from `outputPrefix`, timestamps, then `MASTERFILE_TAG_TIMESTAMP.ndjson`.  
6. **Stream-concatenate**: Pipes each shard into a write stream, preserving order.  
7. **Finalize**: Ends write stream, then (optionally) deletes shard files.  
8. **Respond**: Returns `{ masterFile, mergedCount }`.

---

## Prerequisites

- **Node.js 20.x**  
- **Google Cloud Functions API** enabled  
- **IAM**: Function’s service account needs:
  - `roles/storage.objectViewer` on the bucket  
  - `roles/storage.objectCreator` on the bucket  

---

## Local Development & Testing

1. **Install deps**  
   ```bash
   cd functions/merge_shards
   npm install
````

2. **Emulate HTTP locally** (via `functions-framework`):

   ```bash
   npm install @google-cloud/functions-framework
   npx functions-framework --target=mergeShards
   ```

3. **Invoke locally**:

   ```bash
   curl -X POST localhost:8080/ \
     -H "Content-Type: application/json" \
     -d '{
       "prefix":"horse_data/",
       "outputPrefix":"horse_data/",
       "pattern":"^shard_.*\\.ndjson$"
     }'
   ```

---

## Deploying

From the `functions/merge_shards` directory:

```bash
gcloud functions deploy mergeShards \
  --region=europe-central2 \
  --runtime=nodejs20 \
  --trigger-http \
  --entry-point=mergeShards \
  --source=. \
  --set-env-vars=BUCKET_NAME=horse-predictor-v2-data \
  --allow-unauthenticated
```

---

## Invoking / Running

### Via `gcloud functions call`

```bash
gcloud functions call mergeShards \
  --region=europe-central2 \
  --data '{
    "prefix":"jockey_data/",
    "outputPrefix":"jockey_data/",
    "pattern":"^shard_.*\\.ndjson$"
  }'
```

### Via `curl`

```bash
URL=$(gcloud functions describe mergeShards \
       --region=europe-central2 \
       --format="value(httpsTrigger.url)")

curl -X POST $URL \
  -H "Content-Type: application/json" \
  -d '{
    "prefix":"jockey_data/",
    "outputPrefix":"jockey_data/",
    "pattern":"^shard_.*\\.ndjson$"
  }'
```

---

## Logging & Error Handling

* **Console.debug**: Steps & shard counts
* **Console.info**: “No shards to merge” or final success
* **Console.warn**: Missing inputs or early exit conditions
* **Console.error**: Read/write failures, unexpected exceptions

Use Cloud Logging filters to track `mergeShards` invocations, error rates, and merged‐shard counts.

---

## Next Steps

1. **Uncomment shard deletion** once validated in production.
2. **Add monitoring**: Create logs-based metrics/alerts on `❌` / error patterns.
3. **Integrate**: Call `mergeShards` from Cloud Workflows after each scraper step.
4. **Clean & Ingest**: Chain with `cleanMaster` → ingestion jobs for full pipeline.

```
