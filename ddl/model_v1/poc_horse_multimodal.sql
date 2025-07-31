-- Create POC multiclass model with bucketed features for non-linear effects
CREATE OR REPLACE MODEL `horse-predictor-v2.horse_data_v2.poc_horse_multiclass_v1`
OPTIONS(
  model_type               = 'LOGISTIC_REG',
  input_label_cols         = ['finish_place'],
  data_split_method        = 'RANDOM',
  data_split_eval_fraction = 0.2,
  max_iterations           = 50
) AS
SELECT
  -- Continuous numeric features
  distance_m,
  SAFE_CAST(temp_c AS FLOAT64)        AS temp_c,
  rest_days,
  jockey_weight_kg,
  prize_amount,
  -- Bucketed features for non-linear effects
  distance_bucket,
  temp_bucket,
  prize_amount_bucket,
  rest_bucket,

  -- Career metrics
  horse_race_count,
  horse_win_pct,
  career_race_count,
  career_win_pct,
  career_prize_sum,

  -- Entity metrics
  jockey_race_count,
  jockey_win_pct,
  trainer_race_count,
  trainer_win_pct,
  breeder_race_count,
  breeder_win_pct,

  -- Synergy & specialization
  hj_podium_rate,
  ht_podium_rate,
  hb_podium_rate,
  win_pct_1600,
  win_pct_2000,
  win_pct_delta_rain,
  avg_place_delta_temp,

      -- Pedigree metrics
  father_race_count,
  father_win_count,
  father_win_pct,

  -- Weather & track flags & track flags
  is_rainy,
  is_sunny,
  is_hot,
  is_cloudy,
  is_foggy,
  track_surface_cat,
  category_id,
  race_group,
  subtype,

  -- IDs for lookup-based feature store use (if needed)
  breeder_id,
  jockey_id,
  trainer_id,
  horse_id,
  father_id,

  -- Label
  CAST(finish_place AS STRING) AS finish_place

FROM `horse-predictor-v2.horse_data_v2.race_features_poc`;

-- Insert this run's metrics into the history table
INSERT INTO `horse-predictor-v2.horse_data_v2.model_metrics_history` (
  model_name,
  trained_at,
  accuracy,
  log_loss,
  roc_auc
)
SELECT
  'poc_horse_multiclass_v1' AS model_name, 
  CURRENT_TIMESTAMP()       AS trained_at,
  accuracy,
  log_loss,
  roc_auc
FROM
  ML.EVALUATE(
    MODEL `horse-predictor-v2.horse_data_v2.poc_horse_multiclass_v1`
  );
