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

## Full Features List

[1]  race_id                           Unique identifier for the race.
[2]  horse_id                          Unique identifier for the horse.
[3]  jockey_id                         Unique identifier for the jockey.
[4]  trainer_id                        Unique identifier for the trainer.
[5]  horse_age_years                   Age of the horse in years at model_date.
[6]  is_stallion                       1 if the horse is a stallion; else 0.
[7]  is_mare                           1 if the horse is a mare; else 0.
[8]  is_gelding                        1 if the horse is a gelding; else 0.
[9]  breed_thoroughbred                1 if the horse’s breed = THOROUGHBRED; else 0.
[10] breed_arabian                     1 if breed = ARABIAN; else 0.
[11] breed_standardbred                1 if breed = STANDARDBRED; else 0.
[12] breed_anglo_arabian               1 if breed = ANGLO_ARABIAN; else 0.
[13] distance_m                        Continuous race distance in meters.
[14] dist_l1200                        1 if distance ≤ 1200 m; else 0.
[15] dist_1200_1799                    1 if 1201–1799 m; else 0.
[16] dist_1800_2399                    1 if 1800–2399 m; else 0.
[17] dist_2400_3000                    1 if 2400–3000 m; else 0.
[18] dist_m3000                        1 if distance > 3000 m; else 0.
[19] temperature_c                     Continuous ambient temperature (°C).
[20] is_rainy                          1 if weather indicates rain; else 0.
[21] is_sunny                          1 if weather indicates sun; else 0.
[22] is_hot                            1 if weather = “upalnie” (heat); else 0.
[23] is_cloudy                         1 if weather indicates clouds; else 0.
[24] is_foggy                          1 if weather indicates fog; else 0.
[25] style_ease_score                  Numeric “ease” score of race_style (1 = hardest … 8 = easiest).
[26] group_I                           1 if race_group = GROUP_I; else 0.
[27] group_II                          1 if race_group = GROUP_II; else 0.
[28] group_III                         1 if GROUP_III; else 0.
[29] group_IV                          1 if GROUP_IV; else 0.
[30] group_NONE                        1 if GROUP_NONE; else 0.
[31] group_SLED                        1 if SLED; else 0.
[32] group_HURDLE                      1 if HURDLE; else 0.
[33] group_STEEPLECHASE                1 if STEEPLECHASE; else 0.
[34] group_TRIAL                       1 if TRIAL; else 0.
[35] race_breed_thoroughbred           (aliased) same as horse breed_thoroughbred for the race context.
[36] race_breed_arabian                (aliased) same as above for ARABIAN.
[37] race_breed_standardbred           (aliased) same as above for STANDARDBRED.
[38] race_breed_anglo_arabian          (aliased) same as above for ANGLO_ARABIAN.
[39] country_PL                        1 if race is in Poland; else 0.
[40] city_Warsaw                       1 if race city = Warsaw; else 0.
[41] surface_lekko_elastyczny          1 if track_type = ‘lekko elastyczny’; else 0.
[42] surface_elastyczny                1 if ‘elastyczny’; else 0.
[43] surface_mocno_elastyczny          1 if ‘mocno elastyczny’; else 0.
[44] surface_lekki                     1 if ‘lekki’; else 0.
[45] surface_dobry                     1 if ‘dobry’; else 0.
[46] surface_miekki                    1 if ‘miękki’; else 0.
[47] surface_ciezki                    1 if ‘ciężki’; else 0.
[48] jockey_total_starts               Total career starts for the jockey up to model_date.
[49] jockey_total_wins                 Total career wins for the jockey up to model_date.
[50] jockey_win_pct                    Career win percentage of the jockey.
[51] jockey_win_pct_last_30d           Jockey’s win-% over the past 30 days.
[52] jockey_win_pct_last_60d           Win-% over the past 60 days.
[53] jockey_win_pct_surface_lekkoelastyczny  Jockey’s win-% on ‘lekko elastyczny’ surfaces.
[54] jockey_win_pct_surface_elastyczny        … on ‘elastyczny’.
[55] jockey_win_pct_surface_mocnoelastyczny  … on ‘mocno elastyczny’.
[56] jockey_win_pct_surface_lekki             … on ‘lekki’.
[57] jockey_win_pct_surface_dobry             … on ‘dobry’.
[58] jockey_win_pct_surface_miekki            … on ‘miękki’.
[59] jockey_win_pct_surface_ciezki            … on ‘ciężki’.
[60] jockey_win_pct_dist_l1200         Jockey’s win-% at ≤ 1200 m.
[61] jockey_win_pct_dist_1200_1799     Win-% at 1201–1799 m.
[62] jockey_win_pct_dist_1800_2399     Win-% at 1800–2399 m.
[63] jockey_win_pct_dist_2400_3000     Win-% at 2400–3000 m.
[64] jockey_win_pct_dist_m3000         Win-% at > 3000 m.
[65] licence_country                   Jockey’s country (licence_country code).
[66] trainer_total_starts              Trainer’s total career starts.
[67] trainer_total_wins                Trainer’s total career wins.
[68] trainer_win_pct                   Trainer’s career win percentage.
[69] trainer_win_pct_last_30d          Trainer’s win-% over the past 30 days.
[70] trainer_win_pct_last_60d          Trainer’s win-% over the past 60 days.
[71] trainer_win_pct_surface_lekkoelastyczny … on ‘lekko elastyczny’.
[72] trainer_win_pct_surface_elastyczny … on ‘elastyczny’.
[73] trainer_win_pct_surface_mocnoelastyczny … on ‘mocno elastyczny’.
[74] trainer_win_pct_surface_lekki … on ‘lekki’.
[75] trainer_win_pct_surface_dobry … on ‘dobry’.
[76] trainer_win_pct_surface_miekki … on ‘miękki’.
[77] trainer_win_pct_surface_ciezki … on ‘ciężki’.
[78] trainer_win_pct_dist_l1200        Trainer’s win-% at ≤ 1200 m.
[79] trainer_win_pct_dist_1200_1799    Win-% at 1201–1799 m.
[80] trainer_win_pct_dist_1800_2399    Win-% at 1800–2399 m.
[81] trainer_win_pct_dist_2400_3000    Win-% at 2400–3000 m.
[82] trainer_win_pct_dist_m3000        Win-% at > 3000 m.
[83] trainer_active_horses             Number of active horses currently trained.
[84] breeder_progeny_count             Number of progeny for this horse’s breeder.
[85] breeder_total_progeny_race_count  Sum of career starts across all progeny.
[86] breeder_total_progeny_win_count   Sum of career wins across all progeny.
[87] breeder_avg_win_pct               Average career win-% across all progeny.
[88] breeder_avg_earnings              Average lifetime earnings across all progeny.
[89] breeder_progeny_race_count_last_1yr  Progeny starts in last 365 days.
[90] breeder_progeny_win_count_last_1yr   Progeny wins in last 365 days.
[91] breeder_win_pct_progeny_last_1yr      Progeny win-% over last 365 days.
[92] payout_zwc                        Win‐bet payoff parsed from payments (ZWC).
[93] payout_pdk                        Exact‐2 payoff parsed (PDK).
[94] payout_trj                        Exact‐3 payoff parsed (TRJ).
[95] field_size                        Number of starters in the race.
[96] start_order                       Raw draw position (1…field_size).
[97] jockey_weight_kg                  Assigned weight to jockey (kg) for that race.
[98] median_jockey_weight_kg           Median jockey weight in that race.
[99] weight_diff                       Difference: assigned – median weight.
[100] finish_place                     Final placing (1,2,3…N).
[101] is_win                           1 if finish_place = 1; else 0.
[102] is_place                         1 if finish_place ≤ 3; else 0.
[103] is_paid                          1 if finish_place ≤ 5; else 0.
 

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
