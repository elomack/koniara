-- features/horses_perf.sql
-- Table-valued function computing career starts, wins, and win percentages for sire, dam, trainer, and breeder

CREATE OR REPLACE TABLE FUNCTION `horse-predictor-v2.horse_data_v2.horses_perf`(
  model_date DATE  -- Reference date to include only past races
)
RETURNS TABLE<
  horse_id INT64,
  sire_starts     INT64,
  sire_wins       INT64,
  sire_win_pct    FLOAT64,
  dam_starts      INT64,
  dam_wins        INT64,
  dam_win_pct     FLOAT64,
  trainer_starts  INT64,
  trainer_wins    INT64,
  trainer_win_pct FLOAT64,
  breeder_starts  INT64,
  breeder_wins    INT64,
  breeder_win_pct FLOAT64
> AS (
  WITH records_up_to_date AS (
    -- Filter race records by date up to model_date
    SELECT r.*
    FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` r
    JOIN `horse-predictor-v2.horse_data_v2.RACES` ra
      ON r.race_id = ra.race_id
    WHERE DATE(ra.race_date) <= model_date
  ),
  -- Sire performance: aggregate stats of sire horse itself
  sire_agg AS (
    SELECT
      r.horse_id AS parent_id,
      COUNT(*) AS sire_starts,
      COUNTIF(r.finish_place = 1) AS sire_wins,
      SAFE_DIVIDE(COUNTIF(r.finish_place = 1), COUNT(*)) AS sire_win_pct
    FROM records_up_to_date r
    -- Only consider records where the horse is a sire of any horse
    WHERE r.horse_id IN (
      SELECT DISTINCT father_id FROM `horse-predictor-v2.horse_data_v2.HORSES` WHERE father_id IS NOT NULL
    )
    GROUP BY parent_id
  ),
  -- Dam performance: aggregate stats of dam horse itself
  dam_agg AS (
    SELECT
      r.horse_id AS parent_id,
      COUNT(*) AS dam_starts,
      COUNTIF(r.finish_place = 1) AS dam_wins,
      SAFE_DIVIDE(COUNTIF(r.finish_place = 1), COUNT(*)) AS dam_win_pct
    FROM records_up_to_date r
    -- Only consider records where the horse is a dam of any horse
    WHERE r.horse_id IN (
      SELECT DISTINCT mother_id FROM `horse-predictor-v2.horse_data_v2.HORSES` WHERE mother_id IS NOT NULL
    )
    GROUP BY parent_id
  ),
  -- Trainer performance: aggregate performance grouped by trainer
  trainer_agg AS (
    SELECT
      r.trainer_id AS parent_id,
      COUNT(*) AS trainer_starts,
      COUNTIF(r.finish_place = 1) AS trainer_wins,
      SAFE_DIVIDE(COUNTIF(r.finish_place = 1), COUNT(*)) AS trainer_win_pct
    FROM records_up_to_date r
    WHERE r.trainer_id IS NOT NULL
    GROUP BY parent_id
  ),
  -- Breeder performance: aggregate performance grouped by breeder
  breeder_agg AS (
    SELECT
      h.breeder_id AS parent_id,
      COUNT(*) AS breeder_starts,
      COUNTIF(r.finish_place = 1) AS breeder_wins,
      SAFE_DIVIDE(COUNTIF(r.finish_place = 1), COUNT(*)) AS breeder_win_pct
    FROM records_up_to_date r
    JOIN `horse-predictor-v2.horse_data_v2.HORSES` h
      ON r.horse_id = h.horse_id
    WHERE h.breeder_id IS NOT NULL
    GROUP BY parent_id
  )
  -- Assemble per-horse metrics by joining parent stats
  SELECT
    h.horse_id,
    COALESCE(sp.sire_starts, 0)     AS sire_starts,
    COALESCE(sp.sire_wins, 0)       AS sire_wins,
    COALESCE(sp.sire_win_pct, 0)    AS sire_win_pct,
    COALESCE(dp.dam_starts, 0)      AS dam_starts,
    COALESCE(dp.dam_wins, 0)        AS dam_wins,
    COALESCE(dp.dam_win_pct, 0)     AS dam_win_pct,
    COALESCE(tp.trainer_starts, 0)  AS trainer_starts,
    COALESCE(tp.trainer_wins, 0)    AS trainer_wins,
    COALESCE(tp.trainer_win_pct, 0) AS trainer_win_pct,
    COALESCE(bp.breeder_starts, 0)  AS breeder_starts,
    COALESCE(bp.breeder_wins, 0)    AS breeder_wins,
    COALESCE(bp.breeder_win_pct, 0) AS breeder_win_pct
  FROM `horse-predictor-v2.horse_data_v2.HORSES` h
  LEFT JOIN sire_agg    sp ON h.father_id  = sp.parent_id
  LEFT JOIN dam_agg     dp ON h.mother_id  = dp.parent_id
  LEFT JOIN trainer_agg tp ON h.trainer_id = tp.parent_id
  LEFT JOIN breeder_agg bp ON h.breeder_id = bp.parent_id
);

-- Usage example:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.horses_perf`('2025-07-31');
