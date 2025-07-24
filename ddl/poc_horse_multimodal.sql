CREATE OR REPLACE MODEL
  `horse-predictor-v2.horse_data_v2.poc_horse_multiclass`
OPTIONS(
  model_type               = 'LOGISTIC_REG',
  input_label_cols         = ['finish_place'],
  data_split_method        = 'RANDOM',
  data_split_eval_fraction = 0.2,
  max_iterations           = 50
)
AS
SELECT
  distance_m,
  rest_days,
  jockey_weight_kg,
  prize_amount,
  horse_race_count,
  horse_win_pct,
  career_race_count,
  career_win_pct,
  career_prize_sum,
  jockey_race_count,
  jockey_win_pct,
  trainer_race_count,
  trainer_win_pct,
  breeder_race_count,
  breeder_win_pct,
  hj_podium_rate,
  ht_podium_rate,
  win_pct_1600,
  win_pct_2000,
  win_pct_delta_rain,
  avg_place_delta_temp,
  father_race_count,
  father_win_count,
  father_win_pct,
  temp_bucket,
  is_rainy,
  is_sunny,
  is_hot,
  is_cloudy,
  is_foggy,
  track_type_cat,
  category_id,
  race_group,
  subtype,
  breeder_id,
  jockey_id,
  trainer_id,
  horse_id,
  father_id,
  CAST(finish_place AS STRING) AS finish_place
FROM
  `horse-predictor-v2.horse_data_v2.race_features_poc`;
