# 🧑‍🌾 Trainer Data Scraper

A Cloud Run–based batch job that scrapes trainer records from the public Homas API and writes newline-delimited JSON (NDJSON) shards into Google Cloud Storage.

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

This service fetches trainer metadata in ID-based batches. It:

- Spawns up to **10 concurrent** HTTP requests  
- Automatically **halts** after **10 consecutive** “404 Not Found” responses (end-of-database cutoff)  
- Emits emoji-enhanced `console.debug` / `console.warn` / `console.error` logs  
- Writes each batch as an NDJSON shard under:

```

gs\://\<BUCKET\_NAME>/trainer\_data/shard\_<startId>*<endId>*\<YYYY\_MM\_DD\_hh\:mm\:ss>.ndjson

````

---

## Features

- **Batchable** via `startId` & `batchSize`  
- **Concurrency control** using a promise-pool (`CONCURRENCY_LIMIT = 10`)  
- **404 cutoff** logic (`CUTOFF = 10`)  
- **Rich, emoji-driven logging** for clarity in Cloud Logging  
- **Fully parameterized** for bucket name, concurrency, and cutoff values  

---

## Prerequisites

- **Node.js 20.x**  
- A Google Cloud project with:
  - **Cloud Run Admin**  
  - **Storage Object Admin**  
- A GCS bucket (e.g. `horse-predictor-v2-data`)  

Dependencies (in `package.json`):
```json
{
  "@google-cloud/storage": "^7.x",
  "axios": "^1.x",
  "p-limit": "^6.x"
}
````

---

## Configuration

| Environment Variable | Description                      | Default                   |
| -------------------- | -------------------------------- | ------------------------- |
| `BUCKET_NAME`        | GCS bucket for NDJSON shards     | `horse-predictor-v2-data` |
| `CONCURRENCY_LIMIT`  | Max parallel HTTP fetches        | `10`                      |
| `CUTOFF`             | Consecutive 404s before stopping | `10`                      |

Override via Cloud Run’s `--set-env-vars` flag.

---

## Local Development & Testing

1. **Install dependencies**

   ```bash
   cd services/trainer_scraper-v2
   npm install
   ```

2. **Run a small batch**

   ```bash
   node index.js 1 50
   ```

3. **Check output & logs**

   ```bash
   node index.js 1 50 > out.ndjson 2> debug.log
   tail -n 30 debug.log
   ```

---

## Building & Deploying

From the repo root:

```bash
cd services/trainer_scraper-v2

# Build Docker image
docker build -t gcr.io/<PROJECT_ID>/trainer-scraper-job:v1 .

# Push to Google Container Registry
docker push gcr.io/<PROJECT_ID>/trainer-scraper-job:v1
```

---

## Running as a Cloud Run Job

1. **Enable Cloud Run**

   ```bash
   gcloud services enable run.googleapis.com
   ```

2. **Deploy the Job**

   ```bash
   gcloud run jobs deploy trainer-scraper-job \
     --image=gcr.io/<PROJECT_ID>/trainer-scraper-job:v1 \
     --region=europe-central2 \
     --service-account=pipeline-runner@<PROJECT_ID>.iam.gserviceaccount.com \
     --set-env-vars=BUCKET_NAME=horse-predictor-v2-data,CONCURRENCY_LIMIT=10,CUTOFF=10 \
     --args="\${startId},\${batchSize}" \
     --max-retries=3 \
     --parallelism=10 \
     --tasks=1
   ```

3. **Execute**

   ```bash
   gcloud run jobs execute trainer-scraper-job \
     --region=europe-central2 \
     --args="1,1000"
   ```

4. **View Logs**

   ```bash
   gcloud run jobs logs read trainer-scraper-job \
     --region=europe-central2 --limit=50
   ```

---

## Logging & Monitoring

* All `console.debug` / `console.warn` / `console.error` entries flow into **Cloud Logging**.
* Create logs-based metrics on patterns like `❌` or `⚠️ Miss` to alert on errors or end-of-database detection.

---

## Next Steps

1. **Shard Merge**: Invoke `mergeShards` with `prefix="trainer_data/"`.
2. **Clean & Dedupe**: Run `cleanMaster` on the merged masterfile.
3. **Ingest into BigQuery**: Load cleaned NDJSON into the `TRAINERS` reference table.
4. **Workflow Orchestration**: Chain scraping → merging → cleaning → ingestion in a Cloud Workflow.

```
