-- ddl/assemble_features.sql
-- Assemble horse_features and race_features using modular feature functions and views

-- 1) Horse-level feature TVF for global rating model
CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horse_features`(
  model_date DATE
)
RETURNS TABLE<
  horse_id INT64,
  age_years INT64,
  is_age_outlier BOOL,
  is_stallion BOOL,
  is_mare BOOL,
  is_gelding BOOL,
  breed_thoroughbred BOOL,
  breed_arabian BOOL,
  breed_standardbred BOOL,
  breed_anglo_arabian BOOL,
  color_gniada BOOL,
  color_siwa BOOL,
  color_kasztanowata BOOL,
  color_ciemnogniada BOOL,
  color_skarogniada BOOL,
  color_kara BOOL,
  sire_starts INT64,
  sire_wins INT64,
  sire_win_pct FLOAT64,
  dam_starts INT64,
  dam_wins INT64,
  dam_win_pct FLOAT64,
  trainer_starts INT64,
  trainer_wins INT64,
  trainer_win_pct FLOAT64,
  breeder_starts INT64,
  breeder_wins INT64,
  breeder_win_pct FLOAT64,
  career_starts INT64,
  career_wins INT64,
  career_win_pct FLOAT64,
  total_earnings FLOAT64,
  earnings_per_start FLOAT64,
  rolling_race_count_last_3yrs INT64,
  rolling_win_count_last_3yrs INT64,
  rolling_win_pct_last_3yrs FLOAT64,
  race_count_current_year INT64,
  win_count_current_year INT64,
  win_pct_current_year FLOAT64,
  current_vs_last3yrs_win_pct FLOAT64,
  days_since_last_race INT64
> AS (
  SELECT
    hb.horse_id,
    hb.age_years,
    hb.is_age_outlier,
    hb.is_stallion,
    hb.is_mare,
    hb.is_gelding,
    hb.breed_thoroughbred,
    hb.breed_arabian,
    hb.breed_standardbred,
    hb.breed_anglo_arabian,
    hb.color_gniada,
    hb.color_siwa,
    hb.color_kasztanowata,
    hb.color_ciemnogniada,
    hb.color_skarogniada,
    hb.color_kara,
    hp.sire_starts,
    hp.sire_wins,
    hp.sire_win_pct,
    hp.dam_starts,
    hp.dam_wins,
    hp.dam_win_pct,
    hp.trainer_starts,
    hp.trainer_wins,
    hp.trainer_win_pct,
    hp.breeder_starts,
    hp.breeder_wins,
    hp.breeder_win_pct,
    hc.career_starts,
    hc.career_wins,
    hc.career_win_pct,
    hc.total_earnings,
    hc.earnings_per_start,
    hr.rolling_race_count_last_3yrs,
    hr.rolling_win_count_last_3yrs,
    hr.rolling_win_pct_last_3yrs,
    hr.race_count_current_year,
    hr.win_count_current_year,
    hr.win_pct_current_year,
    hr.current_vs_last3yrs_win_pct,
    hr.days_since_last_race
  FROM
    `horse-predictor-v2.horse_data_v2.horses_base`(model_date) AS hb
  JOIN
    `horse-predictor-v2.horse_data_v2.horses_perf`(model_date) AS hp
    ON hb.horse_id = hp.horse_id
  JOIN
    `horse-predictor-v2.horse_data_v2.horses_career`(model_date) AS hc
    ON hb.horse_id = hc.horse_id
  JOIN
    `horse-predictor-v2.horse_data_v2.horses_recent`(model_date) AS hr
    ON hb.horse_id = hr.horse_id
);

-- 2) Race-level feature view for probability model
CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.race_features` AS
SELECT
  rr.race_id,
  rr.horse_id,
  rr.jockey_id,
  rr.trainer_id,
  hf.age_years         AS horse_age_years,
  hf.is_stallion,
  hf.is_mare,
  hf.is_gelding,
  hf.breed_thoroughbred,
  hf.breed_arabian,
  hf.breed_standardbred,
  hf.breed_anglo_arabian,
  -- Race static features
  rb.distance_m,
  rb.dist_l1200,
  rb.dist_1200_1799,
  rb.dist_1800_2399,
  rb.dist_2400_3000,
  rb.dist_m3000,
  rb.temperature_c,
  rb.is_rainy,
  rb.is_cloudy,
  rb.is_sunny,
  rb.is_hot,
  rb.is_foggy,
  rb.style_ease_score,
  rb.group_I,
  rb.group_II,
  rb.group_III,
  rb.group_IV,
  rb.group_NONE,
  rb.group_SLED,
  rb.group_HURDLE,
  rb.group_STEEPLECHASE,
  rb.group_TRIAL,
  rb.breed_thoroughbred as race_breed_thoroughbred,
  rb.breed_arabian as race_breed_arabian,
  rb.breed_standardbred as race_breed_standardbred,
  rb.breed_anglo_arabian as race_breed_anglo_arabian,
  rb.country_PL,
  rb.city_Warsaw,
  rb.surface_lekko_elastyczny,
  rb.surface_elastyczny,
  rb.surface_mocno_elastyczny,
  rb.surface_lekki,
  rb.surface_dobry,
  rb.surface_miekki,
  rb.surface_ciezki,
  -- Payout features
  rp.payout_zwc,
  rp.payout_pdk,
  rp.payout_dwj,
  rp.payout_trj,
  rp.payout_czw,
  -- Jockey features
  jk.jockey_total_starts,
  jk.jockey_total_wins,
  jk.jockey_win_pct,
  jk.jockey_win_pct_last_30d,
  jk.jockey_win_pct_last_60d,
  jk.jockey_win_pct_surface_lekkoelastyczny,
  jk.jockey_win_pct_surface_elastyczny,
  jk.jockey_win_pct_surface_mocnoelastyczny,
  jk.jockey_win_pct_surface_lekki,
  jk.jockey_win_pct_surface_dobry,
  jk.jockey_win_pct_surface_miekki,
  jk.jockey_win_pct_surface_ciezki,
  jk.jockey_win_pct_dist_l1200,
  jk.jockey_win_pct_dist_1200_1799,
  jk.jockey_win_pct_dist_1800_2399,
  jk.jockey_win_pct_dist_2400_3000,
  jk.jockey_win_pct_dist_m3000,
  -- Trainer features
  tr.trainer_total_starts,
  tr.trainer_total_wins,
  tr.trainer_win_pct,
  tr.trainer_win_pct_last_30d,
  tr.trainer_win_pct_last_60d,
  tr.trainer_win_pct_surface_lekkoelastyczny,
  tr.trainer_win_pct_surface_elastyczny,
  tr.trainer_win_pct_surface_mocnoelastyczny,
  tr.trainer_win_pct_surface_lekki,
  tr.trainer_win_pct_surface_dobry,
  tr.trainer_win_pct_surface_miekki,
  tr.trainer_win_pct_surface_ciezki,
  tr.trainer_win_pct_dist_l1200,
  tr.trainer_win_pct_dist_1200_1799,
  tr.trainer_win_pct_dist_1800_2399,
  tr.trainer_win_pct_dist_2400_3000,
  tr.trainer_win_pct_dist_m3000,
  tr.trainer_active_horses,
  -- Breeder features
  bb.breeder_progeny_count,
  bb.breeder_total_progeny_race_count,
  bb.breeder_total_progeny_win_count,
  bb.breeder_avg_win_pct,
  bb.breeder_avg_earnings,
  bb.breeder_progeny_race_count_last_1yr,
  bb.breeder_progeny_win_count_last_1yr,
  bb.breeder_win_pct_progeny_last_1yr
FROM
  `horse-predictor-v2.horse_data_v2.RACE_RECORDS` AS rr
JOIN
  `horse-predictor-v2.horse_data_v2.horse_features`('2025-07-31') AS hf
  ON rr.horse_id = hf.horse_id
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.jockeys`('2025-07-31') AS jk
  ON rr.jockey_id = jk.jockey_id
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.trainers`('2025-07-31') AS tr
  ON rr.trainer_id = tr.trainer_id
-- Static race features join
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.races_base` AS rb
  ON rr.race_id = rb.race_id
-- Payouts join
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.races_payouts` AS rp
  ON rr.race_id = rp.race_id
-- Link to horses master for breeder lookup
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.HORSES` AS h
  ON rr.horse_id = h.horse_id
LEFT JOIN
  `horse-predictor-v2.horse_data_v2.breeders`('2025-07-31') AS bb
  ON h.breeder_id = bb.breeder_id;
