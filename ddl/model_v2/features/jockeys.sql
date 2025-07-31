-- features/jockeys.sql
-- Table-valued function computing jockey-level performance and specialization features

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.jockeys`(
  model_date DATE  -- Reference date for cutoff
)
RETURNS TABLE<
  jockey_id INT64,
  jockey_total_starts INT64,
  jockey_total_wins INT64,
  jockey_win_pct FLOAT64,
  jockey_win_pct_last_30d FLOAT64,
  jockey_win_pct_last_60d FLOAT64,
  jockey_win_pct_surface_lekkoelastyczny FLOAT64,
  jockey_win_pct_surface_elastyczny FLOAT64,
  jockey_win_pct_surface_mocnoelastyczny FLOAT64,
  jockey_win_pct_surface_lekki FLOAT64,
  jockey_win_pct_surface_dobry FLOAT64,
  jockey_win_pct_surface_miekki FLOAT64,
  jockey_win_pct_surface_ciezki FLOAT64,
  jockey_win_pct_dist_l1200 FLOAT64,
  jockey_win_pct_dist_1200_1799 FLOAT64,
  jockey_win_pct_dist_1800_2399 FLOAT64,
  jockey_win_pct_dist_2400_3000 FLOAT64,
  jockey_win_pct_dist_m3000 FLOAT64
> AS (
  WITH recs AS (
    SELECT
      rr.jockey_id,
      rr.finish_place,
      ra.race_date,
      ra.track_type,
      ra.track_distance_m
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr
    JOIN `horse-predictor-v2.horse_data_v2.RACES` ra
      ON rr.race_id = ra.race_id
    WHERE DATE(ra.race_date) <= model_date
      AND rr.jockey_id IS NOT NULL
  ),
  overall AS (
    SELECT
      jockey_id,
      COUNT(*) AS jockey_total_starts,
      COUNTIF(finish_place = 1) AS jockey_total_wins,
      SAFE_DIVIDE(COUNTIF(finish_place = 1), COUNT(*)) AS jockey_win_pct
    FROM recs
    GROUP BY jockey_id
  ),
  recent_30d AS (
    SELECT
      jockey_id,
      COUNT(*) AS starts_30d,
      COUNTIF(finish_place = 1) AS wins_30d
    FROM recs
    WHERE DATE_DIFF(model_date, DATE(race_date), DAY) <= 30
    GROUP BY jockey_id
  ),
  recent_60d AS (
    SELECT
      jockey_id,
      COUNT(*) AS starts_60d,
      COUNTIF(finish_place = 1) AS wins_60d
    FROM recs
    WHERE DATE_DIFF(model_date, DATE(race_date), DAY) <= 60
    GROUP BY jockey_id
  ),
  surface AS (
    SELECT
      jockey_id,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'lekko elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'lekko elastyczny' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_lekkoelastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'elastyczny' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_elastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'mocno elastyczny' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'mocno elastyczny' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_mocnoelastyczny,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'lekki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'lekki' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_lekki,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'dobry' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'dobry' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_dobry,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'miękki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'miękki' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_miekki,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_type = 'ciężki' AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_type = 'ciężki' THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_surface_ciezki
    FROM recs
    GROUP BY jockey_id
  ),
  distance AS (
    SELECT
      jockey_id,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m < 1200 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m < 1200 THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_dist_l1200,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 1200 AND 1799 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 1200 AND 1799 THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_dist_1200_1799,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 1800 AND 2399 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 1800 AND 2399 THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_dist_1800_2399,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m BETWEEN 2400 AND 3000 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m BETWEEN 2400 AND 3000 THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_dist_2400_3000,
      SAFE_DIVIDE(
        SUM(CASE WHEN track_distance_m >= 3001 AND finish_place = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN track_distance_m >= 3001 THEN 1 ELSE 0 END)
      ) AS jockey_win_pct_dist_m3000
    FROM recs
    GROUP BY jockey_id
  ),
  metrics AS (
    SELECT
      o.jockey_id,
      o.jockey_total_starts,
      o.jockey_total_wins,
      o.jockey_win_pct,
      SAFE_DIVIDE(r30.wins_30d, r30.starts_30d) AS jockey_win_pct_last_30d,
      SAFE_DIVIDE(r60.wins_60d, r60.starts_60d) AS jockey_win_pct_last_60d,
      COALESCE(s.jockey_win_pct_surface_lekkoelastyczny, 0.0) AS jockey_win_pct_surface_lekkoelastyczny,
      COALESCE(s.jockey_win_pct_surface_elastyczny, 0.0) AS jockey_win_pct_surface_elastyczny,
      COALESCE(s.jockey_win_pct_surface_mocnoelastyczny, 0.0) AS jockey_win_pct_surface_mocnoelastyczny,
      COALESCE(s.jockey_win_pct_surface_lekki, 0.0) AS jockey_win_pct_surface_lekki,
      COALESCE(s.jockey_win_pct_surface_dobry, 0.0) AS jockey_win_pct_surface_dobry,
      COALESCE(s.jockey_win_pct_surface_miekki, 0.0) AS jockey_win_pct_surface_miekki,
      COALESCE(s.jockey_win_pct_surface_ciezki, 0.0) AS jockey_win_pct_surface_ciezki,
      COALESCE(d.jockey_win_pct_dist_l1200, 0.0) AS jockey_win_pct_dist_l1200,
      COALESCE(d.jockey_win_pct_dist_1200_1799, 0.0) AS jockey_win_pct_dist_1200_1799,
      COALESCE(d.jockey_win_pct_dist_1800_2399, 0.0) AS jockey_win_pct_dist_1800_2399,
      COALESCE(d.jockey_win_pct_dist_2400_3000, 0.0) AS jockey_win_pct_dist_2400_3000,
      COALESCE(d.jockey_win_pct_dist_m3000, 0.0) AS jockey_win_pct_dist_m3000
    FROM overall o
    LEFT JOIN recent_30d r30 ON o.jockey_id = r30.jockey_id
    LEFT JOIN recent_60d r60 ON o.jockey_id = r60.jockey_id
    LEFT JOIN surface s     ON o.jockey_id = s.jockey_id
    LEFT JOIN distance d    ON o.jockey_id = d.jockey_id
  )
  SELECT
    m.jockey_id,
    m.jockey_total_starts,
    m.jockey_total_wins,
    m.jockey_win_pct,
    m.jockey_win_pct_last_30d,
    m.jockey_win_pct_last_60d,
    m.jockey_win_pct_surface_lekkoelastyczny,
    m.jockey_win_pct_surface_elastyczny,
    m.jockey_win_pct_surface_mocnoelastyczny,
    m.jockey_win_pct_surface_lekki,
    m.jockey_win_pct_surface_dobry,
    m.jockey_win_pct_surface_miekki,
    m.jockey_win_pct_surface_ciezki,
    m.jockey_win_pct_dist_l1200,
    m.jockey_win_pct_dist_1200_1799,
    m.jockey_win_pct_dist_1800_2399,
    m.jockey_win_pct_dist_2400_3000,
    m.jockey_win_pct_dist_m3000
  FROM metrics m
  LEFT JOIN `horse-predictor-v2.horse_data_v2.JOCKEYS` j
    ON m.jockey_id = j.jockey_id
);

-- Usage example:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.jockeys`('2025-07-31');
