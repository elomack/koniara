# 🐎 Horse Data Scraper

A Cloud Run–based batch job that scrapes raw horse records from the public Homas API and writes newline-delimited JSON (NDJSON) shards into Google Cloud Storage.

---

## Table of Contents

1. [Overview](#overview)  
2. [Features](#features)  
3. [Prerequisites](#prerequisites)  
4. [Configuration](#configuration)  
5. [Local Development & Testing](#local-development--testing)  
6. [Building & Deploying](#building--deploying)  
7. [Running as a Cloud Run Job](#running-as-a-cloud-run-job)  
8. [Logging & Monitoring](#logging--monitoring)  
9. [Next Steps](#next-steps)  

---

## Overview

This service fetches horse metadata, career history, and race records in ID-based batches. It:

- Spawns up to **10 concurrent** requests  
- Automatically **halts** after **10 consecutive** “404 Not Found” responses (end-of-database cutoff)  
- Emits richly-annotated `console.debug` / `console.warn` / `console.error` logs  
- Outputs each batch as an NDJSON shard under a GCS prefix:

```

gs\://\<BUCKET\_NAME>/horse\_data/shard\_<startId>*<endId>*\<YYYY\_MM\_DD\_hh\:mm\:ss>.ndjson

````

---

## Features

- **Batchable** via `startId` & `batchSize`  
- **Concurrency control** using a promise-pool  
- **404 cutoff logic** to avoid endless polling  
- **Emoji-enhanced logs** for easy debugging in Cloud Logging  
- **Parameterized**: bucket name, concurrency, cutoff are all configurable  

---

## Prerequisites

- Node.js 20.x  
- A Google Cloud project with:
  - **Cloud Run Admin**  
  - **Storage Object Admin** roles  
  - A GCS bucket (e.g. `horse-predictor-v2-data`)  

- The `@google-cloud/storage`, `axios` & `p-limit` npm packages

---

## Configuration

| Environment Variable | Description                                         | Default                         |
| -------------------- | --------------------------------------------------- | ------------------------------- |
| `BUCKET_NAME`        | GCS bucket to write NDJSON shards into             | `horse-predictor-v2-data`       |
| `CONCURRENCY_LIMIT`  | Max parallel HTTP fetches                           | `10`                            |
| `CUTOFF`             | Consecutive 404s before halting the batch           | `10`                            |

You can override these via the Cloud Run job’s `--set-env-vars` flag.

---

## Local Development & Testing

1. **Install dependencies**  
   ```bash
   cd services/horse_data_scraper-v2
   npm install
````

2. **Run a small batch locally**

   ```bash
   # <startId> <batchSize>
   node index.js 1 50
   ```

3. **Inspect logs**

   ```bash
   # Redirect stdout and stderr to files
   node index.js 1 50 > out.ndjson 2> debug.log
   tail -n 30 debug.log
   ```

---

## Building & Deploying

This service uses a Dockerfile for Cloud Run. From the repo root:

```bash
cd services/horse_data_scraper-v2

# Build container
docker build -t gcr.io/<PROJECT_ID>/horse-data-scraper-job:v1 .

# Push to Google Container Registry
docker push gcr.io/<PROJECT_ID>/horse-data-scraper-job:v1
```

---

## Running as a Cloud Run Job

1. **Enable APIs**

   ```bash
   gcloud services enable run.googleapis.com
   ```

2. **Deploy Job**

   ```bash
   gcloud run jobs deploy horse-data-scraper-job \
     --image=gcr.io/<PROJECT_ID>/horse-data-scraper-job:v1 \
     --region=europe-central2 \
     --service-account=pipeline-runner@<PROJECT_ID>.iam.gserviceaccount.com \
     --set-env-vars=BUCKET_NAME=horse-predictor-v2-data \
     --args="\${startId},\${batchSize}" \
     --max-retries=3 \
     --parallelism=10 \
     --tasks=1
   ```

3. **Execute**

   ```bash
   gcloud run jobs execute horse-data-scraper-job \
     --region=europe-central2 \
     --args="1,1000"
   ```

4. **View Logs**

   ```bash
   gcloud run jobs logs read horse-data-scraper-job \
     --region=europe-central2 --limit=50
   ```

---

## Logging & Monitoring

* **Cloud Logging** captures all `console.debug`, `console.warn`, and `console.error` streams.
* **Metrics**: you can create logs-based metrics on patterns like `❌ Error` or `⚠️ Miss #` to alert on scraper failures or end-of-database conditions.

---

## Next Steps

1. **Merge Shards**: use the `mergeShards` Cloud Function to concatenate NDJSON batches.
2. **Clean & Dedupe**: deploy the `cleanMaster` job to sanitize and remove duplicates.
3. **Ingest to BigQuery**: spin up the ingestion jobs for staging → upsert into your V2 tables.
4. **Full Orchestration**: wire everything together in a Cloud Workflow with proper retries and alerting.

---

*“Lock down your core data transforms first, then build the mini-workflow for validation before stitching together the full end-to-end pipeline.”*
