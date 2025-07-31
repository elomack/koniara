-- features/horses_recent.sql
-- Table-valued function computing recent-form features:
-- rolling counts and rates over last 3 years, current year counts and rates,
-- comparison between current year win pct and last 3-year average, plus days since last race

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_recent`(
  model_date DATE  -- Reference date for as-of calculations
)
RETURNS TABLE<
  horse_id INT64,
  rolling_race_count_last_3yrs INT64,
  rolling_win_count_last_3yrs INT64,
  rolling_win_pct_last_3yrs FLOAT64,
  race_count_current_year INT64,
  win_count_current_year INT64,
  win_pct_current_year FLOAT64,
  current_vs_last3yrs_win_pct FLOAT64,
  days_since_last_race INT64
> AS (
  WITH 
  -- Annual aggregated stats per horse-year up to model_date
  career_rates AS (
    SELECT
      horse_id,
      race_year,
      SUM(SAFE_CAST(race_count AS INT64)) AS annual_race_count,
      SUM(SAFE_CAST(race_won_count AS INT64)) AS annual_win_count,
      SAFE_DIVIDE(
        SUM(SAFE_CAST(race_won_count AS INT64)),
        SUM(SAFE_CAST(race_count AS INT64))
      ) AS annual_win_rate
    FROM
      `horse-predictor-v2.horse_data_v2.HORSE_CAREERS`
    WHERE
      race_year <= EXTRACT(YEAR FROM model_date)
    GROUP BY horse_id, race_year
  ),
  -- Filter to last 3 years including current model_date year
  last3 AS (
    SELECT *
    FROM career_rates
    WHERE race_year BETWEEN EXTRACT(YEAR FROM model_date) - 2 AND EXTRACT(YEAR FROM model_date)
  ),
  -- Aggregate over last 3 years
  last3_agg AS (
    SELECT
      horse_id,
      SUM(annual_race_count) AS rolling_race_count_last_3yrs,
      SUM(annual_win_count)  AS rolling_win_count_last_3yrs,
      SAFE_DIVIDE(SUM(annual_win_count), SUM(annual_race_count)) AS rolling_win_pct_last_3yrs
    FROM last3
    GROUP BY horse_id
  ),
  -- Current year stats
  current_year AS (
    SELECT
      horse_id,
      SAFE_CAST(race_count AS INT64)     AS race_count_current_year,
      SAFE_CAST(race_won_count AS INT64) AS win_count_current_year,
      SAFE_DIVIDE(race_won_count, race_count) AS win_pct_current_year
    FROM
      `horse-predictor-v2.horse_data_v2.HORSE_CAREERS`
    WHERE
      race_year = EXTRACT(YEAR FROM model_date)
  ),
  -- Recency of last race
  recency AS (
    SELECT
      rr.horse_id,
      DATE_DIFF(model_date, MAX(DATE(ra.race_date)), DAY) AS days_since_last_race
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr
    JOIN `horse-predictor-v2.horse_data_v2.RACES` ra
      ON rr.race_id = ra.race_id
    WHERE DATE(ra.race_date) <= model_date
    GROUP BY rr.horse_id
  )
  SELECT
    h.horse_id,
    COALESCE(l3.rolling_race_count_last_3yrs, 0)   AS rolling_race_count_last_3yrs,
    COALESCE(l3.rolling_win_count_last_3yrs, 0)    AS rolling_win_count_last_3yrs,
    COALESCE(l3.rolling_win_pct_last_3yrs, 0.0)    AS rolling_win_pct_last_3yrs,
    COALESCE(cy.race_count_current_year, 0)        AS race_count_current_year,
    COALESCE(cy.win_count_current_year, 0)         AS win_count_current_year,
    COALESCE(cy.win_pct_current_year, 0.0)         AS win_pct_current_year,
    SAFE_SUBTRACT(
      COALESCE(cy.win_pct_current_year, 0.0),
      COALESCE(l3.rolling_win_pct_last_3yrs, 0.0)
    )                                              AS current_vs_last3yrs_win_pct,
    COALESCE(rc.days_since_last_race, NULL)        AS days_since_last_race
  FROM `horse-predictor-v2.horse_data_v2.HORSES` h
  LEFT JOIN last3_agg l3   ON h.horse_id = l3.horse_id
  LEFT JOIN current_year cy ON h.horse_id = cy.horse_id
  LEFT JOIN recency rc      ON h.horse_id = rc.horse_id
);

-- Usage example:
-- SELECT *
-- FROM `horse-predictor-v2.horse_data_v2.horses_recent`('2025-07-31');
