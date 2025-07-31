-- features/breeders.sql
-- Table-valued function computing breeder-level progeny volume, success, and recent form

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.breeders`(
  model_date DATE  -- Reference date for cutoff
)
RETURNS TABLE<
  breeder_id INT64,
  breeder_progeny_count INT64,
  breeder_total_progeny_race_count INT64,
  breeder_total_progeny_win_count INT64,
  breeder_avg_win_pct FLOAT64,
  breeder_avg_earnings FLOAT64,
  breeder_progeny_race_count_last_1yr INT64,
  breeder_progeny_win_count_last_1yr INT64,
  breeder_win_pct_progeny_last_1yr FLOAT64
> AS (
  WITH progeny AS (
    -- List each progeny with its breeder
    SELECT
      horse_id AS progeny_id,
      breeder_id
    FROM `horse-predictor-v2.horse_data_v2.HORSES`
    WHERE breeder_id IS NOT NULL
  ),
  career_stats AS (
    -- Pull career-level aggregates for each progeny
    SELECT
      p.breeder_id,
      c.horse_id AS progeny_id,
      c.career_starts,
      c.career_wins,
      c.career_win_pct,
      c.total_earnings
    FROM progeny p
    JOIN `horse-predictor-v2.horse_data_v2.horses_career`(model_date) AS c
      ON p.progeny_id = c.horse_id
  ),
  aggregated AS (
    -- Compute breeder-wide sums and averages
    SELECT
      breeder_id,
      COUNT(progeny_id)                             AS breeder_progeny_count,
      SUM(career_starts)                            AS breeder_total_progeny_race_count,
      SUM(career_wins)                              AS breeder_total_progeny_win_count,
      SAFE_DIVIDE(AVG(career_win_pct), 1)           AS breeder_avg_win_pct,
      SAFE_DIVIDE(AVG(total_earnings), 1)           AS breeder_avg_earnings
    FROM career_stats
    GROUP BY breeder_id
  ),
  recent AS (
    -- Recent progeny form over the last 1 year
    SELECT
      p.breeder_id,
      COUNT(*)                                     AS starts_1yr,
      COUNTIF(r.finish_place = 1)                  AS wins_1yr
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` r
    JOIN `horse-predictor-v2.horse_data_v2.RACES` ra
      ON r.race_id = ra.race_id
    JOIN progeny p
      ON r.horse_id = p.progeny_id
    WHERE
      DATE(ra.race_date) <= model_date
      AND DATE(ra.race_date) > DATE_SUB(model_date, INTERVAL 1 YEAR)
    GROUP BY p.breeder_id
  ),
  recent_pct AS (
    SELECT
      breeder_id,
      starts_1yr AS breeder_progeny_race_count_last_1yr,
      wins_1yr   AS breeder_progeny_win_count_last_1yr,
      SAFE_DIVIDE(wins_1yr, starts_1yr) AS breeder_win_pct_progeny_last_1yr
    FROM recent
  )
  -- Combine all breeder metrics
  SELECT
    a.breeder_id,
    a.breeder_progeny_count,
    a.breeder_total_progeny_race_count,
    a.breeder_total_progeny_win_count,
    a.breeder_avg_win_pct,
    a.breeder_avg_earnings,
    COALESCE(rp.breeder_progeny_race_count_last_1yr, 0) AS breeder_progeny_race_count_last_1yr,
    COALESCE(rp.breeder_progeny_win_count_last_1yr, 0)  AS breeder_progeny_win_count_last_1yr,
    COALESCE(rp.breeder_win_pct_progeny_last_1yr, 0.0) AS breeder_win_pct_progeny_last_1yr
  FROM aggregated a
  LEFT JOIN recent_pct rp
    ON a.breeder_id = rp.breeder_id
);

-- Usage example:
-- SELECT *
-- FROM `horse-predictor-v2.horse_data_v2.breeders`('2025-07-31');
