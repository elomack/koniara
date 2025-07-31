-- features/trainers.sql
-- Table-valued function computing trainer-level performance, specialization, stable size, and nationality

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.trainers`(
  model_date DATE  -- Reference date for cutoff
)
RETURNS TABLE<
  trainer_id INT64,
  trainer_total_starts INT64,
  trainer_total_wins INT64,
  trainer_win_pct FLOAT64,
  trainer_win_pct_last_30d FLOAT64,
  trainer_win_pct_last_60d FLOAT64,
  trainer_win_pct_surface_lekkoelastyczny FLOAT64,
  trainer_win_pct_surface_elastyczny FLOAT64,
  trainer_win_pct_surface_mocnoelastyczny FLOAT64,
  trainer_win_pct_surface_lekki FLOAT64,
  trainer_win_pct_surface_dobry FLOAT64,
  trainer_win_pct_surface_miekki FLOAT64,
  trainer_win_pct_surface_ciezki FLOAT64,
  trainer_win_pct_dist_l1200 FLOAT64,
  trainer_win_pct_dist_1200_1799 FLOAT64,
  trainer_win_pct_dist_1800_2399 FLOAT64,
  trainer_win_pct_dist_2400_3000 FLOAT64,
  trainer_win_pct_dist_m3000 FLOAT64,
  trainer_active_horses INT64
> AS (
  WITH recs AS (
    SELECT
      rr.trainer_id,
      rr.finish_place,
      ra.race_date,
      ra.track_type,
      ra.track_distance_m
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr
    JOIN `horse-predictor-v2.horse_data_v2.RACES` ra
      ON rr.race_id = ra.race_id
    WHERE DATE(ra.race_date) <= model_date
      AND rr.trainer_id IS NOT NULL
  ),
  overall AS (
    SELECT
      trainer_id,
      COUNT(*) AS trainer_total_starts,
      COUNTIF(finish_place = 1) AS trainer_total_wins,
      SAFE_DIVIDE(COUNTIF(finish_place = 1), COUNT(*)) AS trainer_win_pct
    FROM recs
    GROUP BY trainer_id
  ),
  recent_30d AS (
    SELECT
      trainer_id,
      COUNT(*) AS starts_30d,
      COUNTIF(finish_place = 1) AS wins_30d
    FROM recs
    WHERE DATE_DIFF(model_date, DATE(race_date), DAY) <= 30
    GROUP BY trainer_id
  ),
  recent_60d AS (
    SELECT
      trainer_id,
      COUNT(*) AS starts_60d,
      COUNTIF(finish_place = 1) AS wins_60d
    FROM recs
    WHERE DATE_DIFF(model_date, DATE(race_date), DAY) <= 60
    GROUP BY trainer_id
  ),
  surface AS (
    SELECT
      trainer_id,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'lekko elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'lekko elastyczny' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_lekkoelastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'elastyczny' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_elastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'mocno elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'mocno elastyczny' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_mocnoelastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'lekki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'lekki' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_lekki,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'dobry' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'dobry' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_dobry,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'miękki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'miękki' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_miekki,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'ciężki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'ciężki' THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_surface_ciezki
    FROM recs
    GROUP BY trainer_id
  ),
  distance AS (
    SELECT
      trainer_id,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m <= 1200 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m <= 1200 THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_dist_l1200,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 1201 AND 1799 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 1201 AND 1799 THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_dist_1200_1799,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 1800 AND 2399 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 1800 AND 2399 THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_dist_1800_2399,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 2400 AND 3000 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 2400 AND 3000 THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_dist_2400_3000,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m > 3000 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m > 3000 THEN 1 ELSE 0 END)
      ) AS trainer_win_pct_dist_m3000
    FROM recs
    GROUP BY trainer_id
  ),
  stable AS (
    SELECT
      trainer_id,
      COUNT(DISTINCT horse_id) AS trainer_active_horses
    FROM `horse-predictor-v2.horse_data_v2.HORSES`
    GROUP BY trainer_id
  ),
  metrics AS (
    SELECT
      o.trainer_id,
      o.trainer_total_starts,
      o.trainer_total_wins,
      o.trainer_win_pct,
      SAFE_DIVIDE(r30.wins_30d, r30.starts_30d) AS trainer_win_pct_last_30d,
      SAFE_DIVIDE(r60.wins_60d, r60.starts_60d) AS trainer_win_pct_last_60d,
      COALESCE(s.trainer_win_pct_surface_lekkoelastyczny, 0.0) AS trainer_win_pct_surface_lekkoelastyczny,
      COALESCE(s.trainer_win_pct_surface_elastyczny, 0.0)    AS trainer_win_pct_surface_elastyczny,
      COALESCE(s.trainer_win_pct_surface_mocnoelastyczny, 0.0) AS trainer_win_pct_surface_mocnoelastyczny,
      COALESCE(s.trainer_win_pct_surface_lekki, 0.0)         AS trainer_win_pct_surface_lekki,
      COALESCE(s.trainer_win_pct_surface_dobry, 0.0)         AS trainer_win_pct_surface_dobry,
      COALESCE(s.trainer_win_pct_surface_miekki, 0.0)        AS trainer_win_pct_surface_miekki,
      COALESCE(s.trainer_win_pct_surface_ciezki, 0.0)        AS trainer_win_pct_surface_ciezki,
      COALESCE(d.trainer_win_pct_dist_l1200, 0.0)            AS trainer_win_pct_dist_l1200,
      COALESCE(d.trainer_win_pct_dist_1200_1799, 0.0)       AS trainer_win_pct_dist_1200_1799,
      COALESCE(d.trainer_win_pct_dist_1800_2399, 0.0)       AS trainer_win_pct_dist_1800_2399,
      COALESCE(d.trainer_win_pct_dist_2400_3000, 0.0)       AS trainer_win_pct_dist_2400_3000,
      COALESCE(d.trainer_win_pct_dist_m3000, 0.0)           AS trainer_win_pct_dist_m3000,
      COALESCE(st.trainer_active_horses, 0)                 AS trainer_active_horses
    FROM overall o
    LEFT JOIN recent_30d r30 ON o.trainer_id = r30.trainer_id
    LEFT JOIN recent_60d r60 ON o.trainer_id = r60.trainer_id
    LEFT JOIN surface s     ON o.trainer_id = s.trainer_id
    LEFT JOIN distance d    ON o.trainer_id = d.trainer_id
    LEFT JOIN stable st     ON o.trainer_id = st.trainer_id
  )
  SELECT
    m.*
  FROM metrics m
  LEFT JOIN `horse-predictor-v2.horse_data_v2.TRAINERS` tr
    ON m.trainer_id = tr.trainer_id
);

-- Usage example:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.trainers`('2025-07-31');
