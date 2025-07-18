# 🧹 cleanMaster – NDJSON Cleaner & Deduplicator

A Cloud Run–based job that downloads a master NDJSON file from GCS, streams & filters it to:

- **Drop** malformed JSON lines  
- **Drop** duplicate records by `id`  
- **Re-upload** a cleaned, deduplicated NDJSON back to GCS  

---

## Table of Contents

1. [Overview](#overview)  
2. [Input & Environment](#input--environment)  
3. [Behavior & Flow](#behavior--flow)  
4. [Prerequisites](#prerequisites)  
5. [Local Development & Testing](#local-development--testing)  
6. [Building & Deploying](#building--deploying)  
7. [Running as a Cloud Run Job](#running-as-a-cloud-run-job)  
8. [Logging & Monitoring](#logging--monitoring)  
9. [Next Steps](#next-steps)  

---

## Overview

`cleanMaster.js` takes a large NDJSON “master” file, streams it line by line, filters out:

- Lines that aren’t valid JSON  
- Records without an `id` field  
- Duplicate `id` values  

And writes a cleaned version back to the same GCS folder with a suffix:  
```

<originalName>*cleaned\_deduped*<timestamp>.ndjson

````

---

## Input & Environment

This job reads two **required** environment variables:

| Env Var         | Description                                  |
| --------------- | -------------------------------------------- |
| `BUCKET_NAME`   | GCS bucket name (e.g. `horse-predictor-v2-data`)    |
| `MASTER_FILE`   | Path to the NDJSON file in that bucket       |

Example:
```bash
export BUCKET_NAME=horse-predictor-v2-data
export MASTER_FILE=horse_data/MASTERFILE_HORSEDATA_2025_07_16_14_30_00.ndjson
````

---

## Behavior & Flow

1. **Download** the file to local temp directory
2. **Stream** it line-by-line via `readline`
3. **Parse** each line:

   * If JSON and has an `id` & not seen before → write to output
   * Otherwise → count as malformed or duplicate
4. **Finish** writing and **upload** the cleaned file back to GCS under the same folder
5. **Report** statistics: total, kept, malformed, duplicates

---

## Prerequisites

* Node.js 20.x
* **Cloud Run** and **Storage APIs** enabled
* Service account with:

  * `storage.objects.get` & `storage.objects.create` on bucket
  * `storage.objects.list` (for folder writes)

Dependencies (in `package.json`):

```json
{
  "@google-cloud/storage": "^7.x"
}
```

---

## Local Development & Testing

1. **Install deps**

   ```bash
   cd services/data_ingestion_service
   npm install
   ```

2. **Set env vars**

   ```bash
   export BUCKET_NAME=horse-predictor-v2-data
   export MASTER_FILE="horse_data/MASTERFILE_HORSEDATA_2025_07_16_14_30_00.ndjson"
   ```

3. **Run locally**

   ```bash
   node src/cleanMaster.js
   ```

4. **Inspect logs** and verify cleaned file appears in bucket.

---

## Building & Deploying

From the repo root:

```bash
cd services/data_ingestion_service

# Build Docker container
docker build -t gcr.io/<PROJECT_ID>/clean-master-job:v1 .

# Push to Container Registry
docker push gcr.io/<PROJECT_ID>/clean-master-job:v1
```

---

## Running as a Cloud Run Job

1. **Enable APIs**

   ```bash
   gcloud services enable run.googleapis.com
   ```

2. **Deploy the Job**

   ```bash
   gcloud run jobs deploy clean-master-job \
     --image=gcr.io/<PROJECT_ID>/clean-master-job:v1 \
     --region=europe-central2 \
     --service-account=pipeline-runner@<PROJECT_ID>.iam.gserviceaccount.com \
     --set-env-vars=BUCKET_NAME=horse-predictor-v2-data,MASTER_FILE=horse_data/MASTERFILE_HORSEDATA_<timestamp>.ndjson \
     --args "" \
     --max-retries=1 \
     --parallelism=1 \
     --tasks=1
   ```

3. **Execute**

   ```bash
   gcloud run jobs execute clean-master-job \
     --region=europe-central2
   ```

4. **View Logs**

   ```bash
   gcloud run jobs logs read clean-master-job \
     --region=europe-central2 --limit=50
   ```

---

## Logging & Monitoring

* **console.log** reports totals, malformed, duplicates, and cleaned file path.
* Create logs-based metrics on “malformed” or “duplicate” counts to monitor data quality.

---

## Next Steps

1. **Integrate** into your Cloud Workflow after `mergeShards`.
2. **Parameterize** input vs. output file naming if needed.
3. **Alerting**: add an alert if malformed or duplicate counts exceed thresholds.
4. **Enable parallelism** for very large files (split & merge strategy).

```
