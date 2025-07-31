-- features/horses_base.sql
-- Table-valued function computing static horse-level features: horse_id, age_years, age_outlier flag, and sex one-hot encodings

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_base`(
  model_date DATE  -- Reference date for age calculation
)
RETURNS TABLE<
  horse_id INT64,         -- Unique horse identifier
  age_years INT64,        -- Horse age in years as of model_date
  is_age_outlier BOOL,    -- Flags implausible ages (<0 or >20)
  is_stallion BOOL,       -- True if horse_sex = 'STALLION'
  is_mare BOOL,           -- True if horse_sex = 'MARE'
  is_gelding BOOL         -- True if horse_sex = 'GELDING'
> AS (
  SELECT
    horse_id,

    -- Calculate age in years by subtracting birth_year from the model_date's year
    EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) AS age_years,

    -- Flag ages that are outliers: negative or over 20 years
    (
      EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) < 0
      OR EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) > 20
    ) AS is_age_outlier,

    -- One-hot encoding of horse sex
    CASE
      WHEN horse_sex = 'STALLION' THEN TRUE
      ELSE FALSE
    END AS is_stallion,

    CASE
      WHEN horse_sex = 'MARE' THEN TRUE
      ELSE FALSE
    END AS is_mare,

    CASE
      WHEN horse_sex = 'GELDING' THEN TRUE
      ELSE FALSE
    END AS is_gelding

  FROM
    `horse-predictor-v2.horse_data_v2.HORSES`  -- Source table with raw horse metadata
);

-- Usage example:
--   SELECT *
--   FROM `horse-predictor-v2.horse_data_v2.horses_base`('2025-07-31');
