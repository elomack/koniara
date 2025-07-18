# Ingest Service

This document describes the **Ingest Cloud Function** (`functions/ingest/index.js`), which:

1. **Discovers** new cleaned NDJSON files under a given GCS prefix.
2. **Flattens** nested arrays for `horse_data/` (HORSES, HORSE\_CAREERS, RACES, RACE\_RECORDS).
3. **Loads** updated rows for reference tables (BREEDERS, JOCKEYS, TRAINERS) via staging+MERGE.
4. **Maintains** audit timestamps (`created_date`, `last_updated_date`).
5. **Updates** the `ingestion_metadata` table watermark.

---

## Prerequisites

* Node.js 20 runtime
* GCP project with:

  * Cloud Functions API enabled
  * BigQuery dataset `horse_data_v2` and tables created via the DDLs in `ddl/`
  * GCS bucket (e.g. `horse-predictor-v2-data`) containing cleaned NDJSON files

## Schema Definitions

All target tables are defined in the `ddl/` folder. Ensure these DDLs have been applied before running ingest:

* `ddl/BREEDERS.sql`
* `ddl/HORSE_CAREERS.sql`
* `ddl/HORSES.sql`
* `ddl/ingestion_metadata.sql`
* `ddl/JOCKEYS.sql`
* `ddl/RACE_RECORDS.sql`
* `ddl/RACES.sql`
* `ddl/TRAINERS.sql`

## Configuration

The function reads two environment variables:

* `BUCKET_NAME` — Name of the GCS bucket where cleaned files are stored
* `BQ_DATASET`  — BigQuery dataset (e.g. `horse_data_v2`)

## Deployment

From the `functions/ingest` directory:

```bash
npm install

gcloud functions deploy ingest \
  --region=europe-central2 \
  --runtime=nodejs20 \
  --trigger-http \
  --entry-point=ingest \
  --source=. \
  --set-env-vars=BUCKET_NAME=horse-predictor-v2-data,BQ_DATASET=horse_data_v2 \
  --allow-unauthenticated
```

## Invocation

Invoke the function by specifying the GCS prefix to ingest:

```bash
gcloud functions call ingest \
  --region=europe-central2 \
  --data='{"prefix":"horse_data/"}'
```

Supported prefixes:

* `horse_data/`
* `breeder_data/`
* `jockey_data/`
* `trainer_data/`

## Behavior & Logging

* Logs key steps with `INFO`, `DEBUG`, and `ERROR` levels.
* On success, returns JSON:

  ```json
  {
    "prefix": "horse_data/",
    "count": 42,
    "lastProcessedTime": "2025-07-18T15:30:00.000Z"
  }
  ```
* HTTP 204 if no new files; 400 on bad input; 500 on errors.
---
