# 🐄 Breeder Data Scraper

A Cloud Run–based batch job that scrapes breeder records from the public Homas API and writes newline-delimited JSON (NDJSON) shards into Google Cloud Storage.

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

This service fetches breeder metadata in ID-based batches. It:

- Spawns up to **10 concurrent** HTTP requests  
- Automatically **halts** after **10 consecutive** “404 Not Found” responses (end-of-database cutoff)  
- Emits emoji-annotated `console.debug` / `console.warn` / `console.error` logs  
- Writes each batch as an NDJSON shard under:

```

gs\://\<BUCKET\_NAME>/breeder\_data/shard\_<startId>*<endId>*\<YYYY\_MM\_DD\_hh\:mm\:ss>.ndjson

````

---

## Features

- **Batchable** via `startId` & `batchSize`  
- **Concurrency control** with a promise-pool (`CONCURRENCY_LIMIT = 10`)  
- **404 cutoff** logic (`CUTOFF = 10`)  
- **Rich logging** for easy debugging  
- **Parameterized**: bucket name, concurrency, and cutoff are all configurable  

---

## Prerequisites

- Node.js 20.x  
- A Google Cloud project with:
  - **Cloud Run Admin**  
  - **Storage Object Admin** roles  
- A GCS bucket (e.g. `horse-predictor-v2-data`)  

Dependencies (in `package.json`):
```json
{
  "@google-cloud/storage": "…",
  "axios": "…",
  "p-limit": "…"
}
````

---

## Configuration

| Environment Variable | Description                      | Default                   |
| -------------------- | -------------------------------- | ------------------------- |
| `BUCKET_NAME`        | GCS bucket for NDJSON shards     | `horse-predictor-v2-data` |
| `CONCURRENCY_LIMIT`  | Max parallel HTTP fetches        | `10`                      |
| `CUTOFF`             | Consecutive 404s before stopping | `10`                      |

Override via Cloud Run’s `--set-env-vars`.

---

## Local Development & Testing

1. **Install dependencies**

   ```bash
   cd services/breeder_scraper-v2
   npm install
   ```

2. **Run a small batch locally**

   ```bash
   node index.js 1 50
   ```

3. **Inspect logs & output**

   ```bash
   node index.js 1 50 > out.ndjson 2> debug.log
   tail -n 30 debug.log
   ```

---

## Building & Deploying

From the repo root:

```bash
cd services/breeder_scraper-v2

# Build Docker image
docker build -t gcr.io/<PROJECT_ID>/breeder-scraper-job:v1 .

# Push to Container Registry
docker push gcr.io/<PROJECT_ID>/breeder-scraper-job:v1
```

---

## Running as a Cloud Run Job

1. **Enable Cloud Run API**

   ```bash
   gcloud services enable run.googleapis.com
   ```

2. **Deploy the Job**

   ```bash
   gcloud run jobs deploy breeder-scraper-job \
     --image=gcr.io/<PROJECT_ID>/breeder-scraper-job:v1 \
     --region=europe-central2 \
     --service-account=pipeline-runner@<PROJECT_ID>.iam.gserviceaccount.com \
     --set-env-vars=BUCKET_NAME=horse-predictor-v2-data,CONCURRENCY_LIMIT=10,CUTOFF=10 \
     --args="\${startId},\${batchSize}" \
     --max-retries=3 \
     --parallelism=10 \
     --tasks=1
   ```

3. **Execute a batch**

   ```bash
   gcloud run jobs execute breeder-scraper-job \
     --region=europe-central2 \
     --args="1,1000"
   ```

4. **View logs**

   ```bash
   gcloud run jobs logs read breeder-scraper-job \
     --region=europe-central2 --limit=50
   ```

---

## Logging & Monitoring

* All `console.debug` / `warn` / `error` entries flow into **Cloud Logging**.
* Create logs-based metrics on patterns like `❌ Error` or `⚠️ Miss #` to alert on scraper issues or end-of-database conditions.

---

## Next Steps

1. **Shard Merge**: Use the `mergeShards` Cloud Function with `prefix="breeder_data/"`.
2. **Clean & Dedupe**: Run the `cleanMaster` job on the merged masterfile.
3. **Ingest to BigQuery**: Load cleaned NDJSON into the `BREEDERS` reference table.
4. **Workflow Orchestration**: Chain scraping → merging → cleaning → ingestion in Cloud Workflows.

```
