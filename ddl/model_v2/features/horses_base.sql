-- features/horses_base.sql
-- Table-valued function computing static horse-level features: horse_id, age_years, and age_outlier flag

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_base`(
  model_date DATE
)
RETURNS TABLE<
  horse_id INT64,
  age_years INT64,
  is_age_outlier BOOL
> AS (
  SELECT
    -- Unique horse identifier
    horse_id,
    -- Compute age in years
    EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) AS age_years,
    -- Flag implausible ages (e.g., negative or extremely high, beyond realistic racing ages)
    (EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) < 0
     OR EXTRACT(YEAR FROM model_date) - CAST(birth_year AS INT64) > 20) AS is_age_outlier
  FROM
    `horse-predictor-v2.horse_data_v2.HORSES`
);

-- Usage:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.horses_base`('2025-07-31');
