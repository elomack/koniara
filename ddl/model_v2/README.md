# Horse Predictor v2 — Feature Engineering & MLOps Pipeline

This repository provides a modular, maintainable BigQuery feature engineering pipeline for an end-to-end horse-race prediction system on GCP. All feature definitions are split into logical SQL modules, assembled into two main artifacts:

* **`horse_features(model_date)`** — table-valued function (TVF) for global horse-level features as of a given date.
* **`race_features`** — BigQuery view combining horse, race, jockey, trainer, breeder, payout, and record context/outcome features for per-starter prediction.

---

## Directory Structure

```
model_v2/
└── features/
    ├── assemble_features.sql   # Main assembly script (TVF + VIEW)
    ├── breeders.sql            # Breeder-level progeny metrics
    ├── horses_base.sql         # Static horse demographics & one‑hots
    ├── horses_career.sql       # Career aggregates (starts, wins, earnings)
    ├── horses_perf.sql         # Sire/dam/trainer/breeder career stats
    ├── horses_recent.sql       # Recent-form (rolling win%, counts, recency)
    ├── jockeys.sql             # Jockey career & specialization features
    ├── trainers.sql            # Trainer career & specialization features
    ├── races_base.sql          # Static race attributes (distance, style, group, breed, weather, location)
    ├── races_payouts.sql       # Parsed payout signals (ZWC, PDK, TRJ)
    ├── records_context.sql     # Per-starter context (field size, gate draw, weight diff)
    ├── records_outcomes.sql    # Outcome flags (finish_place, is_win/place/paid)
```

---

## Feature Modules Overview

### 1. Horse Features

* **`horses_base.sql`**

  * Age, sex, breed, color one-hot flags
  * Outlier filtering (max age)

* **`horses_perf.sql`**

  * Sire/dam own career starts, wins, win% (via RACE\_RECORDS)
  * Trainer/breeder starts, wins, win%

* **`horses_career.sql`**

  * Career-to-date aggregates: `career_starts`, `career_wins`, `career_win_pct`, `total_earnings`, `earnings_per_start`

* **`horses_recent.sql`**

  * Last-3-year rolling counts & rates
  * Current year counts & rates
  * `current_vs_last3yrs_win_pct`
  * `days_since_last_race`

### 2. Jockey & Trainer Specialized Features

* **`jockeys.sql`**

  * Career starts, wins, win% (overall, 30d, 60d)
  * Surface & distance band win% specialization
  * `licence_country`

* **`trainers.sql`**

  * Same structure as jockeys: career totals, recent-form, surface & distance bands
  * `trainer_active_horses`

### 3. Race Static Attributes

* **`races_base.sql`**

  * **Distance**: continuous + 5 buckets
  * **Temperature**: continuous + 5 weather flags (`is_rainy`, `is_sunny`, `is_hot`, `is_cloudy`, `is_foggy`)
  * **Style ease**: 1–8 mapping of `race_style`
  * **Race group**: one-hot for 9 categories
  * **Breed**: 4 breed flags
  * **Location**: `country_PL`, `city_Warsaw`
  * **Track type**: 7 surface flags

* **`races_payouts.sql`**

  * Parsed payout floats: `payout_zwc`, `payout_pdk`, `payout_trj`

### 4. Per-Record Context & Outcomes

* **`records_context.sql`**

  * `field_size`, `start_order`
  * Jockey weight & `weight_diff`

* **`records_outcomes.sql`**

  * `finish_place` (integer)
  * Binary flags: `is_win`, `is_place`, `is_paid`

---

## Main Assembly

**`assemble_features.sql`** defines:

1. **`horse_features(model_date)`** (TVF): joins **horses\_base**, **horses\_perf**, **horses\_career**, **horses\_recent**.
2. **`race_features`** (VIEW): joins

   * Base static race features (`races_base`, `races_payouts`)
   * Per-record context (`records_context`)
   * Horse features (`horse_features`)
   * Jockey (`jockeys`), Trainer (`trainers`), Breeder (`breeders`)
   * Outcome flags (`records_outcomes`) — for training targets or diagnostics.

---

## Deployment & Smoke Tests

From the `features/` directory, run:

```bash
# Deploy feature modules
bq query --use_legacy_sql=false < breeders.sql
bq query --use_legacy_sql=false < horses_base.sql
bq query --use_legacy_sql=false < horses_perf.sql
bq query --use_legacy_sql=false < horses_career.sql
bq query --use_legacy_sql=false < horses_recent.sql
bq query --use_legacy_sql=false < jockeys.sql
bq query --use_legacy_sql=false < trainers.sql
bq query --use_legacy_sql=false < races_base.sql
bq query --use_legacy_sql=false < races_payouts.sql
bq query --use_legacy_sql=false < records_context.sql
bq query --use_legacy_sql=false < records_outcomes.sql

# Assemble and deploy TVF + view
bq query --use_legacy_sql=false < assemble_features.sql
```

Then sample each view/TVF:

```bash
# Horse features as of 2025-07-31
bq query --use_legacy_sql=false '
  SELECT * FROM `... .horse_features`("2025-07-31") LIMIT 5;'

# Race features snapshot
bq query --use_legacy_sql=false '
  SELECT * FROM `... .race_features` LIMIT 10;'
```
