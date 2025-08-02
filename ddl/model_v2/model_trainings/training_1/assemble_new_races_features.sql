CREATE OR REPLACE TABLE
  `horse-predictor-v2.horse_data_v2.features_SLZ_03082025` AS

WITH inp AS (
  SELECT
    race_number,
    CAST(horse_id  AS STRING) AS horse_id,
    CAST(jockey_id AS STRING) AS jockey_id,
    CAST(trainer_id AS STRING) AS trainer_id,
    temperature_c,
    is_rainy, is_cloudy, is_sunny, is_hot, is_foggy,
    distance_m, dist_l1200, dist_1200_1799, dist_1800_2399, dist_2400_3000, dist_m3000,
    field_size, start_order, jockey_weight_kg,
    group_I, group_II, group_III, group_IV, group_NONE,
    group_SLED, group_HURDLE, group_STEEPLECHASE, group_TRIAL,
    race_breed_thoroughbred, race_breed_arabian,
    race_breed_standardbred, race_breed_anglo_arabian,
    country_PL, city_Warsaw,
    surface_lekko_elastyczny, surface_elastyczny,
    surface_mocno_elastyczny, surface_lekki,
    surface_dobry, surface_miekki, surface_ciezki
  FROM
    `horse-predictor-v2.horse_data_v2.races_SLZ_03082025`
),

h AS (
  SELECT
    CAST(horse_id AS STRING) AS horse_id,
    breeder_id,
    horse_age_years,
    is_stallion, is_mare, is_gelding,
    breed_thoroughbred, breed_arabian,
    breed_standardbred, breed_anglo_arabian
  FROM
    `horse-predictor-v2.horse_data_v2.HORSES`
),

b AS (
  SELECT
    CAST(breeder_id AS STRING) AS breeder_id,
    breeder_progeny_count,
    breeder_total_progeny_race_count,
    breeder_total_progeny_win_count,
    breeder_avg_win_pct,
    breeder_avg_earnings,
    breeder_progeny_race_count_last_1yr,
    breeder_progeny_win_count_last_1yr,
    breeder_win_pct_progeny_last_1yr
  FROM
    `horse-predictor-v2.horse_data_v2.BREEDERS`
),

hf AS (
  SELECT * EXCEPT(snapshot_date)
  FROM `horse-predictor-v2.horse_data_v2.horse_features`(DATE '2025-07-31')
),

j AS (
  SELECT
    CAST(jockey_id AS STRING) AS jockey_id,
    jockey_total_starts, jockey_total_wins, jockey_win_pct,
    jockey_win_pct_last_30d, jockey_win_pct_last_60d,
    jockey_win_pct_surface_lekkoelastyczny, jockey_win_pct_surface_elastyczny,
    jockey_win_pct_surface_mocnoelastyczny, jockey_win_pct_surface_lekki,
    jockey_win_pct_surface_dobry, jockey_win_pct_surface_miekki,
    jockey_win_pct_surface_ciezki,
    jockey_win_pct_dist_l1200, jockey_win_pct_dist_1200_1799,
    jockey_win_pct_dist_1800_2399, jockey_win_pct_dist_2400_3000,
    jockey_win_pct_dist_m3000
  FROM
    `horse-predictor-v2.horse_data_v2.JOCKEYS`
),

t AS (
  SELECT
    CAST(trainer_id AS STRING) AS trainer_id,
    trainer_total_starts, trainer_total_wins, trainer_win_pct,
    trainer_win_pct_last_30d, trainer_win_pct_last_60d,
    trainer_win_pct_surface_lekkoelastyczny, trainer_win_pct_surface_elastyczny,
    trainer_win_pct_surface_mocnoelastyczny, trainer_win_pct_surface_lekki,
    trainer_win_pct_surface_dobry, trainer_win_pct_surface_miekki,
    trainer_win_pct_surface_ciezki,
    trainer_win_pct_dist_l1200, trainer_win_pct_dist_1200_1799,
    trainer_win_pct_dist_1800_2399, trainer_win_pct_dist_2400_3000,
    trainer_win_pct_dist_m3000, trainer_active_horses
  FROM
    `horse-predictor-v2.horse_data_v2.TRAINERS`
)

SELECT
  inp.*,
  h.horse_age_years, h.is_stallion, h.is_mare, h.is_gelding,
  h.breed_thoroughbred, h.breed_arabian,
  h.breed_standardbred, h.breed_anglo_arabian,
  b.breeder_progeny_count, b.breeder_total_progeny_race_count,
  b.breeder_total_progeny_win_count, b.breeder_avg_win_pct,
  b.breeder_avg_earnings, b.breeder_progeny_race_count_last_1yr,
  b.breeder_progeny_win_count_last_1yr, b.breeder_win_pct_progeny_last_1yr,
  hf.* EXCEPT(horse_id),
  j.* EXCEPT(jockey_id),
  t.* EXCEPT(trainer_id)
FROM inp
LEFT JOIN h  USING(horse_id)
LEFT JOIN b  USING(breeder_id)
LEFT JOIN hf USING(horse_id)
LEFT JOIN j  USING(jockey_id)
LEFT JOIN t  USING(trainer_id);
