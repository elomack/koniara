-- Live Predictions SQL for All Upcoming Races (Include Horse Career Stats, Remove is_paid)
--
-- This script loads today's upcoming races from your staging table,
-- enriches them with pre-computed features, calls your BigQuery ML model,
-- and outputs specified fields, including horse career aggregates and
-- the top 12 finish-place probabilities.

-- 1. Define the model date for today's races (used for features TVF)
DECLARE model_date DATE DEFAULT '2025-08-03';

WITH
-- 2. Load staging CSV input and assign a row number for join-back
staging AS (
  SELECT
    ROW_NUMBER() OVER() AS rownum,
    race_number,
    horse_id,
    trainer_id,
    jockey_id,
    jockey_weight_kg,
    start_order
  FROM
    `horse-predictor-v2.horse_data_v2.races_SLZ_03082025`
),

-- 3. Join to features TVF to get engineered features
features AS (
  SELECT
    s.*,                -- staging columns + rownum
    rf.* EXCEPT(
      race_id,
      horse_id,
      trainer_id,
      jockey_id,
      jockey_weight_kg,
      start_order
    )                   -- engineered features only
  FROM
    staging AS s
  JOIN
    `horse-predictor-v2.horse_data_v2.race_features` AS rf
  ON
    rf.horse_id   = s.horse_id
    AND rf.jockey_id  = s.jockey_id
    AND rf.trainer_id = s.trainer_id
),

-- 4. Prepare input for ML.PREDICT by combining staging and engineered features
pred_input AS (
  SELECT
    f.rownum,
    CAST(NULL AS INT64) AS race_id,  -- placeholder
    f.* EXCEPT(rownum)               -- all staging & engineered feature columns
  FROM
    features AS f
),

-- 5. Run ML.PREDICT
pred_raw AS (
  SELECT *
  FROM ML.PREDICT(
    MODEL `horse-predictor-v2.horse_data_v2.train_race_prediction_v1`,
    TABLE pred_input
  )
),

-- 6. Reattach staging identifiers and select core prediction outputs (drop is_paid)
predictions AS (
  SELECT
    s.race_number,
    s.start_order,
    pr.horse_id,
    pr.jockey_id,
    pr.trainer_id,
    pr.horse_age_years,
    pr.jockey_weight_kg,
    pr.jockey_win_pct,
    pr.jockey_win_pct_last_30d,
    pr.jockey_win_pct_last_60d,
    pr.trainer_win_pct,
    pr.trainer_win_pct_last_30d,
    pr.trainer_win_pct_last_60d,
    pr.predicted_finish_place_probs
  FROM
    pred_raw AS pr
  JOIN staging AS s ON pr.rownum = s.rownum
),

-- 7. Aggregate horse career stats from HORSE_CAREERS
horse_stats AS (
  SELECT
    horse_id,
    SUM(race_count)       AS horse_career_starts,
    SUM(race_won_count)   AS horse_career_wins,
    SUM(CASE WHEN race_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN race_count ELSE 0 END)       AS horse_year_starts,
    SUM(CASE WHEN race_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN race_won_count ELSE 0 END)   AS horse_year_wins
  FROM
    `horse-predictor-v2.horse_data_v2.HORSE_CAREERS`
  GROUP BY
    horse_id
),

-- 8. Final output with career stats, names, and separate probability columns
final_output AS (
  SELECT
    p.race_number,
    p.start_order,
    h.horse_name,
    p.horse_age_years,
    p.horse_id,
    hs.horse_career_starts,
    hs.horse_career_wins,
    hs.horse_year_starts,
    hs.horse_year_wins,
    j.last_name            AS jockey_last_name,
    p.jockey_weight_kg,
    p.jockey_win_pct,
    p.jockey_win_pct_last_30d,
    p.jockey_win_pct_last_60d,
    p.jockey_id,
    t.last_name            AS trainer_last_name,
    p.trainer_id,
    p.trainer_win_pct,
    p.trainer_win_pct_last_30d,
    p.trainer_win_pct_last_60d,
    p.predicted_finish_place_probs[OFFSET(0)].prob  AS prob_1,
    p.predicted_finish_place_probs[OFFSET(1)].prob  AS prob_2,
    p.predicted_finish_place_probs[OFFSET(2)].prob  AS prob_3,
    p.predicted_finish_place_probs[OFFSET(3)].prob  AS prob_4,
    p.predicted_finish_place_probs[OFFSET(4)].prob  AS prob_5,
    p.predicted_finish_place_probs[OFFSET(5)].prob  AS prob_6,
    p.predicted_finish_place_probs[OFFSET(6)].prob  AS prob_7,
    p.predicted_finish_place_probs[OFFSET(7)].prob  AS prob_8,
    p.predicted_finish_place_probs[OFFSET(8)].prob  AS prob_9,
    p.predicted_finish_place_probs[OFFSET(9)].prob  AS prob_10,
    p.predicted_finish_place_probs[OFFSET(10)].prob AS prob_11,
    p.predicted_finish_place_probs[OFFSET(11)].prob AS prob_12
  FROM
    predictions AS p
  LEFT JOIN horse_stats AS hs ON p.horse_id = hs.horse_id
  LEFT JOIN `horse-predictor-v2.horse_data_v2.HORSES` AS h ON p.horse_id = h.horse_id
  LEFT JOIN `horse-predictor-v2.horse_data_v2.JOCKEYS` AS j ON p.jockey_id = j.jockey_id
  LEFT JOIN `horse-predictor-v2.horse_data_v2.TRAINERS` AS t ON p.trainer_id = t.trainer_id
)

-- 9. Select and order
SELECT *
FROM final_output
ORDER BY race_number, start_order;
