-- features/horses_career.sql
-- Table-valued function computing career-to-date aggregates from HORSE_CAREERS

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_career`(
  model_date DATE  -- Reference date to include only past seasons
)
RETURNS TABLE<
  horse_id INT64,
  career_starts INT64,
  career_wins INT64,
  career_win_pct FLOAT64,
  total_earnings FLOAT64,
  earnings_per_start FLOAT64
> AS (
  WITH careers_up_to_date AS (
    SELECT
      -- raw fields as strings or numerics
      horse_id,
      SAFE_CAST(race_count AS INT64)     AS race_count,
      SAFE_CAST(race_won_count AS INT64) AS race_won_count,
      SAFE_CAST(prize_amounts AS FLOAT64) AS prize_amounts
    FROM
      `horse-predictor-v2.horse_data_v2.HORSE_CAREERS`
    WHERE
      race_year <= EXTRACT(YEAR FROM model_date)
  ),
  aggregated AS (
    SELECT
      horse_id,
      SUM(race_count)    AS career_starts,
      SUM(race_won_count) AS career_wins,
      SUM(prize_amounts) AS total_earnings
    FROM careers_up_to_date
    GROUP BY horse_id
  )
  SELECT
    a.horse_id,
    a.career_starts,
    a.career_wins,
    SAFE_DIVIDE(a.career_wins, a.career_starts) AS career_win_pct,
    a.total_earnings,
    SAFE_DIVIDE(a.total_earnings, a.career_starts) AS earnings_per_start
  FROM aggregated a
);

-- Usage:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.horses_career`('2025-07-31');
