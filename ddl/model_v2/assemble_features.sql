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
  breeder_win_pct FLOAT64
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
    hp.breeder_win_pct
  FROM
    `horse-predictor-v2.horse_data_v2.horses_base`(model_date) AS hb
  JOIN
    `horse-predictor-v2.horse_data_v2.horses_perf`(model_date) AS hp
    ON hb.horse_id = hp.horse_id
);

-- 2) Race-level feature view for probability model
CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.race_features` AS
SELECT
  rr.race_id,
  rr.horse_id,
  hf.age_years        AS horse_age_years,
  hf.is_stallion,
  hf.is_mare,
  hf.is_gelding,
  hf.breed_thoroughbred,
  hf.breed_arabian,
  hf.breed_standardbred,
  hf.breed_anglo_arabian
  -- future: add distance_m, temperature_c, payout_zwc, etc.
FROM
  (
    -- stub: pull minimal race_records for structure
    SELECT race_id, horse_id
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS`
    LIMIT 1
  ) AS rr
JOIN
  `horse-predictor-v2.horse_data_v2.horse_features`('2025-07-31') AS hf
  ON rr.horse_id = hf.horse_id;
