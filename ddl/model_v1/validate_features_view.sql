# 1. Finish-place coverage
SELECT 
  finish_place,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY finish_place
ORDER BY finish_place;
# • Should see places 1…N (no negatives or NULLs).


# 2. Distance buckets
SELECT 
  distance_bucket,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY distance_bucket
ORDER BY cnt DESC;
# • Buckets: very_short, short, medium, long, very_long, (maybe “unknown”).


#3. Temperature clamping & buckets
SELECT 
  MIN(temp_c) AS min_temp, 
  MAX(temp_c) AS max_temp,
  COUNTIF(temp_c IS NULL) AS null_clamped
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;
# • Min/max should lie in –50…50; all outliers NULL.

SELECT 
  temp_bucket,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY temp_bucket
ORDER BY cnt DESC;
# • Buckets match your defined ranges.


#4. Weather flags sums
SELECT
  SUM(is_rainy)  AS rainy_cnt,
  SUM(is_sunny)  AS sunny_cnt,
  SUM(is_hot)    AS hot_cnt,
  SUM(is_cloudy) AS cloudy_cnt,
  SUM(is_foggy)  AS foggy_cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;
# • Proportions should align with known weather distribution (~10–20% rainy, etc.).


#5. Track-surface categories
SELECT 
  track_surface_cat,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY track_surface_cat
ORDER BY cnt DESC;
# • See exactly your eight Polish surfaces (+ “other”).


#6. Career and prize buckets
SELECT 
  horse_race_count,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY horse_race_count
ORDER BY horse_race_count DESC
LIMIT 10;
# • horse_race_count max should match your largest campaign.

SELECT 
  prize_amount_bucket,
  COUNT(*) AS cnt
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY prize_amount_bucket
ORDER BY cnt DESC;
# • p_0, p_1_1200, … , p_16000+ all present.

#7. Rest-bucket distribution (horse‐centric)
SELECT
  rest_bucket,
  COUNT(DISTINCT horse_id) AS distinct_horses
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
GROUP BY rest_bucket
ORDER BY 
  CASE rest_bucket
    WHEN 'rest_lt_7'   THEN 1
    WHEN 'rest_7_14'   THEN 2
    WHEN 'rest_14_21'  THEN 3
    WHEN 'rest_21_28'  THEN 4
    WHEN 'rest_28_60'  THEN 5
    WHEN 'rest_60_180' THEN 6
    WHEN 'rest_180_365' THEN 7
    ELSE 8
  END;
#• Each horse appears once in its next‐race bucket, with no surprising “other” mass.


#8.Form-momentum validity
WITH only7 AS (
  SELECT 
    win_rate_last_2,
    win_rate_last_7
  FROM `horse-predictor-v2.horse_data_v2.race_features_poc`
  WHERE win_rate_last_7 IS NOT NULL
)
SELECT
  AVG(win_rate_last_2) AS avg_win2_for_7starters,
  AVG(win_rate_last_7) AS avg_win7
FROM only7;
#• Runs without correlated errors and compares 2-race vs 7-race form on the same subset.


#9. Entity counts & win-rates
SELECT
  MIN(jockey_race_count) AS min_jc, MAX(jockey_race_count) AS max_jc,
  MIN(trainer_race_count) AS min_tc, MAX(trainer_race_count) AS max_tc,
  MIN(breeder_race_count) AS min_bc, MAX(breeder_race_count) AS max_bc
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;

SELECT
  AVG(jockey_win_pct) AS avg_jw,
  AVG(trainer_win_pct) AS avg_tw,
  AVG(breeder_win_pct) AS avg_bw
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;
#• Check there are no crazy max values, and that average win‐rates look domain‐reasonable (~10–15%).


#10. Specialty deltas
SELECT
  AVG(win_pct_1600) AS avg_w1600,
  AVG(win_pct_2000) AS avg_w2000,
  AVG(win_pct_delta_rain) AS avg_rain_delta,
  AVG(avg_place_delta_temp) AS avg_temp_delta
FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;
#• Ensure these learned deltas sit in plausible ranges (e.g. small ±%).



