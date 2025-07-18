# 🐴 Koniara: End-to-End Horse Racing Data Pipeline

This repository contains a modular, GCP-native pipeline for scraping, merging, cleaning, and ingesting horse racing data from the public Homas API into BigQuery. It’s composed of:

- **Services** (Cloud Run jobs) for batch scraping:  
  - `horse_data_scraper-v2/` 🐎  
  - `breeder_scraper-v2/` 🐄  
  - `jockey_scraper-v2/` 🏇  
  - `trainer_scraper-v2/` 🧑‍🌾  

- **Functions** for shard management and cleaning:  
  - `merge_shards/` 🔗 — HTTP Cloud Function to concatenate NDJSON shards  
  - `clean_master/` 🧹 — Cloud Run job to drop malformed JSON & dedupe  

- **Ingestion** (Cloud Run) to load cleaned NDJSON into BigQuery staging & upsert into target tables.

- **Workflows** for orchestration (to be implemented): end-to-end chaining of scrape → merge → clean → ingest.

---

## Table of Contents

1. [Architecture & Flow](#architecture--flow)  
2. [Getting Started](#getting-started)  
3. [Directory Layout](#directory-layout)  
4. [Prerequisites](#prerequisites)  
5. [Deploying Components](#deploying-components)  
6. [Testing & Monitoring](#testing-monitoring)  
7. [Next Steps](#next-steps)  

---

## Architecture & Flow

```text
┌────────┐   scrape   ┌────────┐   merge   ┌─────────┐   clean   ┌─────────┐   ingest   ┌────────┐
│ Client │ ─────────> │ Scraper│ ───────> │ merge-  │ ───────> │ clean-  │ ───────> │ Ingest │
│        │            │ (Run)  │          │ shards  │          │ master  │          │ (Run)  │
└────────┘            └────────┘          └─────────┘          └─────────┘          └────────┘
````

1. **Scrape** in small batches (1,000 IDs) with 10× concurrency, halt after 10× 404s.
2. **Merge** per-entity shards into one NDJSON (`mergeShards`).
3. **Clean** the merged file: drop malformed lines & duplicates (`cleanMaster`).
4. **Ingest** cleaned NDJSON into BigQuery (staging→MERGE).
5. (Planned) **Orchestrate** via Cloud Workflows & Cloud Scheduler.

---

## Getting Started

Clone the repo and choose a component:

```bash
git clone https://github.com/elomack/koniara.git
cd koniara
```

Then follow that component’s `README.md` under `services/…` or `functions/…` for detailed build & deploy instructions.

---

## Directory Layout

```
koniara/
├── services/
│   ├── horse_data_scraper-v2/
│   ├── breeder_scraper-v2/
│   ├── jockey_scraper-v2/
│   ├── trainer_scraper-v2/
│   └── data_ingestion_service/
│       └── src/
│           ├── index.js         # ingestion logic
│           └── cleanMaster.js   # NDJSON cleaner
├── functions/
│   └── merge_shards/
│       └── index.js             # mergeShards Cloud Function
├── workflows/
│   └── horse-pipeline-full.yaml # (planned) Cloud Workflow definition
└── README.md                    # ← you are here
```

---

## Prerequisites

* **GCP project** with:

  * Cloud Run, Cloud Functions, Cloud Workflows APIs enabled
  * IAM roles: Cloud Run Admin, Cloud Functions Developer, Storage Admin, BigQuery Admin
* **Node.js 20.x** & **Docker**
* **gcloud CLI** authenticated to your project

---

## Deploying Components

Each component has its own `README.md`. In general:

1. **Scraper Services**

   ```bash
   cd services/horse_data_scraper-v2
   npm install
   docker build -t gcr.io/$PROJECT_ID/horse-scraper:v1 .
   docker push gcr.io/$PROJECT_ID/horse-scraper:v1
   gcloud run jobs deploy horse-scraper-job \
     --image=gcr.io/$PROJECT_ID/horse-scraper:v1 \
     ... (flags vary per service)
   ```

2. **mergeShards Function**

   ```bash
   cd functions/merge_shards
   npm install
   gcloud functions deploy mergeShards \
     --region=europe-central2 \
     --runtime=nodejs20 \
     --trigger-http \
     --entry-point=mergeShards \
     --set-env-vars=BUCKET_NAME=$BUCKET
   ```

3. **cleanMaster Job**

   ```bash
   cd services/data_ingestion_service
   npm install
   docker build -t gcr.io/$PROJECT_ID/clean-master-job:v1 .
   docker push gcr.io/$PROJECT_ID/clean-master-job:v1
   gcloud run jobs deploy clean-master-job ...
   ```

4. **BigQuery Ingestion**

   * See `services/data_ingestion_service/src/index.js` for staging & MERGE logic.

---

## Testing & Monitoring

* **Local**: run each `index.js` with test IDs.
* **Cloud Run & Functions** logs:

  ```bash
  gcloud run jobs logs read <job-name> --region=...  
  gcloud functions logs read mergeShards --region=...
  ```
* **Logs-based metrics**: count “❌” errors or “⚠️ Miss” entries.
* **Alerts**: configure in Cloud Monitoring.

---

## Next Steps

1. **Build & validate** the `cleanMaster` and ingestion jobs end-to-end.
2. **Create mini-Workflows** for each data type: scraper→merge→clean→ingest.
3. **Compose full orchestrator** in `workflows/horse-pipeline-full.yaml`.
4. **Schedule** via Cloud Scheduler.
5. **Cleanup**: enable shard deletion, finalize IAM, add alerts.

> **Guiding principle**: “Lock down your core transforms first, then compose the full Workflow once each stage is hardened.”

```
