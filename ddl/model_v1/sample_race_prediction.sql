-- SQL to dynamically predict a brand-new race with on-the-fly feature computation

-- 1) UDF to strip diacritics (if needed for JOINs)
CREATE TEMP FUNCTION unaccent(s STRING)
  RETURNS STRING
  LANGUAGE js AS """
    var str = s || '';
    return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  """;

WITH
-- 2) Manual entries: horse, jockey, trainer, start position, weight
horse_entries AS (
  SELECT * FROM UNNEST([
STRUCT(42460 AS horse_id, 1147 AS jockey_id, 339 AS trainer_id, 1 AS start_pos, 55 AS jockey_weight_kg),
STRUCT(42500 AS horse_id, 242 AS jockey_id, 256 AS trainer_id, 2 AS start_pos, 55 AS jockey_weight_kg),
STRUCT(42501 AS horse_id, 1115 AS jockey_id, 291 AS trainer_id, 3 AS start_pos, 55 AS jockey_weight_kg),
STRUCT(42502 AS horse_id, 895 AS jockey_id, 211 AS trainer_id, 4 AS start_pos, 56 AS jockey_weight_kg),
STRUCT(42503 AS horse_id, 1188 AS jockey_id, 124 AS trainer_id, 5 AS start_pos, 55 AS jockey_weight_kg)
  ])
),

-- 3) Race-level constants for new race
race_overrides AS (
  SELECT
    1800                AS distance_m,
    'medium'            AS distance_bucket,
    22.0                AS temp_c,
    'temp_15_22'        AS temp_bucket,
    'GROUP_I'           AS race_group,
    NULL                AS category_id,
    CAST(NULL AS STRING) AS subtype,
    'miękki'            AS track_surface_cat,
    1                   AS is_rainy,
    0                   AS is_sunny,
    0                   AS is_cloudy,
    0                   AS is_hot,
    0                   AS is_foggy,
    0.0                 AS prize_amount,
    'p_0'               AS prize_amount_bucket,
    -- Null placeholders for lower-impact features
    NULL                AS breeder_race_count,
    NULL                AS breeder_win_pct,
    NULL                AS hb_podium_rate,
    NULL                AS father_race_count,
    NULL                AS father_win_pct,
    NULL                AS form_momentum_1,
    NULL                AS form_momentum_3,
    NULL                AS form_momentum_5,
    NULL                AS surface_specialty_turf,
    NULL                AS surface_specialty_dirt,
),

-- 4) Horse history metrics
-- 4) Latest horse features from POC view (most recent race per horse)
latest_horse_features AS (
  SELECT
    horse_id,
    horse_race_count,
    horse_win_pct,
    career_prize_sum,
    rest_days,
    rest_bucket,
    (finish_place = 1) AS won_last_race,
    breeder_id,
    father_id
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY horse_id ORDER BY race_date DESC) AS rn
    FROM `horse-predictor-v2.horse_data_v2.race_features_poc` rfp
    join `horse_data_v2.RACES` rac on rac.race_id = rfp.race_id
  )
  WHERE rn = 1
),


-- 5) Jockey history metrics
jockey_metrics AS (
  SELECT
    jockey_id,
    COUNT(*)                         AS jockey_race_count,
    AVG(IF(finish_place = 1, 1, 0))  AS jockey_win_pct,
    AVG(IF(finish_place <= 3, 1, 0))  AS hj_podium_rate
  FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS`
  GROUP BY jockey_id
),

-- 6) Trainer history metrics
trainer_metrics AS (
  SELECT
    trainer_id,
    COUNT(*)                         AS trainer_race_count,
    AVG(IF(finish_place = 1, 1, 0))  AS trainer_win_pct,
    AVG(IF(finish_place <= 3, 1, 0))  AS ht_podium_rate
  FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS`
  GROUP BY trainer_id
),


-- 7) Horse history metrics (full career)
horse_metrics AS (
  SELECT
    horse_id,
    COUNT(*)                         AS horse_race_count,
    AVG(IF(finish_place = 1, 1, 0))  AS horse_win_pct,
    SUM(prize_amount)               AS career_prize_sum
  FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS`
  GROUP BY horse_id
),

-- 8) Assemble feature table for prediction
prediction_input AS (
  SELECT
    he.horse_id,
    h.horse_name,
    he.start_pos,
    -- use model-expected names: horse_race_count, horse_win_pct
    hm.horse_race_count AS career_race_count, 
    hm.horse_race_count,
    hm.horse_win_pct AS career_win_pct, 
    hm.horse_win_pct,
    hm.career_prize_sum,
    lf.rest_days,
    he.jockey_id,
    j.last_name           AS jockey_last_name,
    jm.jockey_race_count,
    jm.jockey_win_pct,
    jm.hj_podium_rate,
    he.trainer_id,
    t.last_name           AS trainer_last_name,
    tm.trainer_race_count,
    tm.trainer_win_pct,
    tm.ht_podium_rate,
    he.jockey_weight_kg,
    lf.rest_bucket,
    ro.distance_m,
    ro.temp_c,
    ro.temp_bucket,
    ro.race_group,
    ro.category_id,
    ro.subtype,
    ro.track_surface_cat,
    ro.is_rainy,
    ro.is_sunny,
    ro.is_cloudy,
    ro.is_hot,
    ro.is_foggy,
    ro.prize_amount,
    ro.prize_amount_bucket,
    ro.distance_bucket,
    ro.breeder_race_count,
    ro.breeder_win_pct,
    ro.hb_podium_rate,
    ro.father_race_count,
    NULL AS father_win_count,
    NULL AS father_win_pct,
    NULL AS win_pct_1600,
    NULL AS win_pct_2000,
    NULL AS win_pct_delta_rain,
    NULL AS avg_place_delta_temp,
    lf.breeder_id,
    lf.father_id
  FROM horse_entries he
  JOIN horse_metrics hm     ON he.horse_id   = hm.horse_id
  JOIN latest_horse_features lf ON he.horse_id = lf.horse_id
  LEFT JOIN jockey_metrics jm   ON he.jockey_id  = jm.jockey_id
  LEFT JOIN trainer_metrics tm  ON he.trainer_id = tm.trainer_id
  JOIN `horse-predictor-v2.horse_data_v2.HORSES`     h ON he.horse_id   = h.horse_id
  JOIN `horse-predictor-v2.horse_data_v2.JOCKEYS`    j ON he.jockey_id  = j.jockey_id
  JOIN `horse-predictor-v2.horse_data_v2.TRAINERS`   t ON he.trainer_id = t.trainer_id
  CROSS JOIN race_overrides ro
)

-- 8) Run BigQuery ML prediction, then normalize & rank by max probability
,
raw AS (
  SELECT
    * EXCEPT(predicted_finish_place, predicted_finish_place_probs),
    predicted_finish_place_probs
  FROM ML.PREDICT(
    MODEL `horse-predictor-v2.horse_data_v2.poc_horse_multiclass_v1`,
    (SELECT * FROM prediction_input)
  )
),

normalized AS (
  SELECT
    r.horse_id,
    r.horse_name,
    r.start_pos,
    r.career_race_count,
    r.career_win_pct,
    r.career_prize_sum,
    r.rest_days,
    r.jockey_last_name,
    r.jockey_race_count,
    r.jockey_win_pct,
    r.hj_podium_rate,
    r.jockey_weight_kg,
    r.trainer_last_name,
    r.trainer_race_count,
    r.trainer_win_pct,
    -- normalize probs for labels 1..8 and build array
    ARRAY(
      SELECT AS STRUCT
        CAST(elem.label AS INT64) AS label,
        elem.prob / tot.sum_prob    AS prob
      FROM UNNEST(r.predicted_finish_place_probs) AS elem
      CROSS JOIN (
        SELECT SUM(e2.prob) AS sum_prob
        FROM UNNEST(r.predicted_finish_place_probs) AS e2
        WHERE CAST(e2.label AS INT64) <= 8
      ) AS tot
      WHERE CAST(elem.label AS INT64) <= 8
      ORDER BY CAST(elem.label AS INT64)
    ) AS filtered_probs,
    -- extract max prob
    (
      SELECT MAX(fp.prob)
      FROM UNNEST(
        ARRAY(
          SELECT AS STRUCT
            CAST(elem.label AS INT64) AS label,
            elem.prob / tot.sum_prob   AS prob
          FROM UNNEST(r.predicted_finish_place_probs) AS elem
          CROSS JOIN (
            SELECT SUM(e2.prob) AS sum_prob
            FROM UNNEST(r.predicted_finish_place_probs) AS e2
            WHERE CAST(e2.label AS INT64) <= 8
          ) AS tot
          WHERE CAST(elem.label AS INT64) <= 8
        )
      ) AS fp
    ) AS max_prob
  FROM raw AS r
)

-- Final SELECT with unique ranking and key metrics
SELECT
  horse_id,
  horse_name,
  start_pos,
  -- top score as percentage
  ROUND(filtered_probs[OFFSET(0)].prob * 100, 2) AS score,
  -- place probabilities
  filtered_probs[OFFSET(0)].prob AS prob_1,
  filtered_probs[OFFSET(1)].prob AS prob_2,
  filtered_probs[OFFSET(2)].prob AS prob_3,
  filtered_probs[OFFSET(3)].prob AS prob_4,
  -- career metrics from full history
  career_race_count   AS horse_race_count,
  ROUND(career_win_pct * 100, 2) AS horse_win_pct,
  career_prize_sum,
  rest_days,
  -- jockey metrics
  jockey_last_name,
  jockey_race_count,
  ROUND(jockey_win_pct * 100, 2) AS jockey_win_pct,
  ROUND(hj_podium_rate * 100, 2) AS hj_podium_rate,
  jockey_weight_kg,
  -- trainer metrics
  trainer_last_name,
  trainer_race_count,
  ROUND(trainer_win_pct * 100, 2) AS trainer_win_pct
FROM normalized
ORDER BY score DESC;
