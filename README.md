# 🐴 Koniara: End-to-End Horse Racing Data Pipeline

This repo implements a fully automated, GCP-native pipeline for scraping, merging, cleaning and ingesting horse racing data into BigQuery. The stages are:

1. **Scrape** public API for Horse / Breeder / Jockey / Trainer records (Cloud Run jobs)  
2. **Merge** per-entity NDJSON shards into a single “master” file (`mergeShards` Cloud Function)  
3. **Clean** the master file: drop malformed lines & dedupe (`cleanMaster` Cloud Run job)  
4. **Ingest** cleaned NDJSON into BigQuery—flattening arrays for `horse_data/` and upserting reference tables via staging+MERGE (`ingest` Cloud Function)  
5. **(Planned)** Orchestrate end-to-end via Cloud Workflows & Cloud Scheduler  

---

## Table of Contents

1. [Architecture & Flow](#architecture--flow)  
2. [Getting Started](#getting-started)  
3. [Directory Layout](#directory-layout)  
4. [Prerequisites](#prerequisites)  
5. [BigQuery DDLs](#bigquery-ddls)  
6. [Deploying Components](#deploying-components)  
7. [Testing & Monitoring](#testing-monitoring)  
8. [Next Steps](#next-steps)  

---

## Architecture & Flow

```text
Scrape ──▶ Merge ──▶ Clean ──▶ Ingest ──▶ BigQuery
   (Run)     (Fn)      (Run)      (Fn)
````

* **Scrape**: batch-parallel jobs (1 000 IDs at a time, 10× concurrency, stop on 10× 404s)
* **Merge**: `mergeShards` HTTP Cloud Function concatenates all `shard_*.ndjson` into a single MASTERFILE
* **Clean**: `cleanMaster` Cloud Run job strips invalid JSON, dedupes, writes `CLEANED_…` files
* **Ingest**: `ingest` HTTP Cloud Function discovers new cleansed files, flattens `horse_data/` arrays, upserts reference tables

---

## Getting Started

```bash
git clone https://github.com/elomack/koniara.git
cd koniara
```

Each component has its own `README.md` under `services/…` or `functions/…`.

---

## Directory Layout

```
koniara/
├── ddl/                           # BigQuery CREATE OR REPLACE TABLE DDLs
│   ├── updated_ddls_with_timestamps.sql
│   └── ingestion_metadata.sql
│
├── services/                      # Cloud Run jobs
│   ├── horse_data_scraper-v2/
│   ├── breeder_scraper-v2/
│   ├── jockey_scraper-v2/
│   └── trainer_scraper-v2/
│
├── functions/                     # HTTP-triggered Cloud Functions
│   ├── merge_shards/              # mergeShards()
│   ├── clean_master/              # cleanMaster()
│   └── ingest/                    # ingest()
│
├── workflows/                     # (Planned) Cloud Workflows definitions
│   └── horse-pipeline-full.yaml
│
└── README.md                      # ← you are here
```

---

## Prerequisites

* **GCP project** with APIs enabled:

  * Cloud Run, Cloud Functions, BigQuery, Cloud Workflows
* **IAM roles**: Cloud Functions Developer, Cloud Run Admin, Storage Admin, BigQuery Admin
* **Node.js 20**, **Docker**, **gcloud CLI**

---

## BigQuery DDLs

All table schemas live in `ddl/`. Run them in order to (re)create:

```bash
# Drop old tables (optional)
PROJECT=horse-predictor-v2
DATASET=horse_data_v2
for T in HORSES HORSE_CAREERS RACES RACE_RECORDS BREEDERS JOCKEYS TRAINERS ingestion_metadata; do
  bq rm -f -t ${PROJECT}:${DATASET}.$T
done

# Create new schemas—with audit fields and descriptions
bq query --use_legacy_sql=false < ddl/updated_ddls_with_timestamps.sql
bq query --use_legacy_sql=false < ddl/ingestion_metadata.sql
```

Each table now includes:

* `created_date TIMESTAMP` – set once on row insertion
* `last_updated_date TIMESTAMP` – updated on every upsert
* Column descriptions via `OPTIONS(description="…")`

---

## Deploying Components

### 1. Scraper Services (Cloud Run jobs)

```bash
cd services/horse_data_scraper-v2
npm install
docker build -t gcr.io/$PROJECT_ID/horse-scraper:v1 .
docker push gcr.io/$PROJECT_ID/horse-scraper:v1
gcloud run jobs deploy horse-scraper-job \
  --image=gcr.io/$PROJECT_ID/horse-scraper:v1 \
  --region=europe-central2 \
  --max-retries=3
```

Repeat for breeder/jockey/trainer scrapers.

---

### 2. mergeShards Function

```bash
cd functions/merge_shards
npm install
gcloud functions deploy mergeShards \
  --region=europe-central2 \
  --runtime=nodejs20 \
  --trigger-http \
  --entry-point=mergeShards \
  --source=. \
  --set-env-vars=BUCKET_NAME=$BUCKET_NAME \
  --allow-unauthenticated
```

---

### 3. cleanMaster Job

```bash
cd functions/clean_master
npm install
gcloud run jobs deploy clean-master-job \
  --image=gcr.io/$PROJECT_ID/clean-master-job:v1 \
  --region=europe-central2 \
  --max-retries=1
```

---

### 4. ingest Function

```bash
cd functions/ingest
npm install
gcloud functions deploy ingest \
  --region=europe-central2 \
  --runtime=nodejs20 \
  --trigger-http \
  --entry-point=ingest \
  --source=. \
  --set-env-vars=BUCKET_NAME=$BUCKET_NAME,BQ_DATASET=$BQ_DATASET \
  --allow-unauthenticated
```

---

## Testing & Monitoring

* **mergeShards**:

  ```bash
  gcloud functions call mergeShards \
    --region=europe-central2 \
    --data='{"prefix":"horse_data/","outputPrefix":"horse_data/","pattern":"^shard_.*\\.ndjson$"}'
  ```
* **cleanMaster**:

  ```bash
  gcloud functions call cleanMaster \
    --region=europe-central2 \
    --data='{"prefix":"horse_data/"}'
  ```
* **ingest**:

  ```bash
  gcloud functions call ingest \
    --region=europe-central2 \
    --data='{"prefix":"horse_data/"}'
  ```

Logs & metrics in Cloud Logging / Monitoring; alerts on ERROR entries.

---

## Next Steps

1. Build per-entity Cloud Workflows chaining scrape→merge→clean→ingest
2. Schedule weekly with Cloud Scheduler
3. Harden production: shard deletion, IAM lockdown, alerts

> **Principle**: Lock down core transforms first, then orchestrate once each stage is validated.

---
