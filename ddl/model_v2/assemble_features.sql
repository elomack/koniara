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
  is_gelding BOOL
> AS (
  SELECT
    hb.horse_id,
    hb.age_years,
    hb.is_age_outlier,
    hb.is_stallion,
    hb.is_mare,
    hb.is_gelding
  FROM
    `horse-predictor-v2.horse_data_v2.horses_base`(model_date) AS hb
);

-- 2) Race-level feature view for probability model
CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.race_features` AS
SELECT
  rr.race_id,
  rr.horse_id,
  hf.age_years AS horse_age_years,
  hf.is_stallion,
  hf.is_mare,
  hf.is_gelding
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
