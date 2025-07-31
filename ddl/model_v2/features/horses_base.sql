-- features/horses_base.sql
-- Table-valued function computing static horse-level features:
-- horse_id, age_years, age_outlier flag, sex, breed, and color one-hot encodings

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_base`(
  model_date DATE  -- Reference date for age calculation
)
RETURNS TABLE<
  horse_id INT64,            -- Unique horse identifier
  age_years INT64,           -- Horse age in years as of model_date
  is_age_outlier BOOL,       -- Flags ages <0 or >20 as outliers
  is_stallion BOOL,          -- True if horse_sex = 'STALLION'
  is_mare BOOL,              -- True if horse_sex = 'MARE'
  is_gelding BOOL,           -- True if horse_sex = 'GELDING'
  breed_thoroughbred BOOL,   -- One-hot: True if breed = 'THOROUGHBRED'
  breed_arabian BOOL,        -- One-hot: True if breed = 'ARABIAN'
  breed_standardbred BOOL,   -- One-hot: True if breed = 'STANDARDBRED'
  breed_anglo_arabian BOOL,  -- One-hot: True if breed = 'ANGLO_ARABIAN'
  color_gniada BOOL,         -- One-hot: True if color_name_pl = 'gniada'
  color_siwa BOOL,           -- One-hot: True if color_name_pl = 'siwa'
  color_kasztanowata BOOL,   -- One-hot: True if color_name_pl = 'kasztanowata'
  color_ciemnogniada BOOL,   -- One-hot: True if color_name_pl = 'ciemnogniada'
  color_skarogniada BOOL,    -- One-hot: True if color_name_pl = 'skarogniada'
  color_kara BOOL            -- One-hot: True if color_name_pl = 'kara'
> AS (
  SELECT
    horse_id,
    -- Age calculation
    EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) AS age_years,
    -- Outlier flag
    (
      EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) < 0
      OR EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) > 20
    ) AS is_age_outlier,
    -- Sex one-hot encodings
    CASE WHEN horse_sex = 'STALLION' THEN TRUE ELSE FALSE END AS is_stallion,
    CASE WHEN horse_sex = 'MARE'    THEN TRUE ELSE FALSE END AS is_mare,
    CASE WHEN horse_sex = 'GELDING' THEN TRUE ELSE FALSE END AS is_gelding,
    -- Breed one-hot encodings
    CASE WHEN breed = 'THOROUGHBRED'   THEN TRUE ELSE FALSE END AS breed_thoroughbred,
    CASE WHEN breed = 'ARABIAN'        THEN TRUE ELSE FALSE END AS breed_arabian,
    CASE WHEN breed = 'STANDARDBRED'   THEN TRUE ELSE FALSE END AS breed_standardbred,
    CASE WHEN breed = 'ANGLO_ARABIAN'  THEN TRUE ELSE FALSE END AS breed_anglo_arabian,
    -- Color one-hot encodings (Polish names)
    CASE WHEN color_name_pl = 'gniada'        THEN TRUE ELSE FALSE END AS color_gniada,
    CASE WHEN color_name_pl = 'siwa'          THEN TRUE ELSE FALSE END AS color_siwa,
    CASE WHEN color_name_pl = 'kasztanowata'  THEN TRUE ELSE FALSE END AS color_kasztanowata,
    CASE WHEN color_name_pl = 'ciemnogniada'  THEN TRUE ELSE FALSE END AS color_ciemnogniada,
    CASE WHEN color_name_pl = 'skarogniada'   THEN TRUE ELSE FALSE END AS color_skarogniada,
    CASE WHEN color_name_pl = 'kara'          THEN TRUE ELSE FALSE END AS color_kara
  FROM
    `horse-predictor-v2.horse_data_v2.HORSES`  -- Source table with raw horse metadata
);

-- Usage example:
-- SELECT *
-- FROM `horse-predictor-v2.horse_data_v2.horses_base`('2025-07-31');
