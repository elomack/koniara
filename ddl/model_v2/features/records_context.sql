-- features/records_context.sql
-- View for per-record contextual features: field size, starting position normalization, and jockey weight differential

CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.records_context` AS
SELECT
  rr.race_id,
  rr.horse_id,

  -- Field size: total number of starters in the race
  COUNT(*) OVER (PARTITION BY rr.race_id) AS field_size,

  -- Starting position
  rr.start_order,

  -- Jockey assigned weight
  rr.jockey_weight_kg,
  -- Median jockey weight in race
  PERCENTILE_CONT(rr.jockey_weight_kg, 0.5) OVER (PARTITION BY rr.race_id) AS median_jockey_weight_kg,
  -- Weight differential: assigned minus median
  rr.jockey_weight_kg - PERCENTILE_CONT(rr.jockey_weight_kg, 0.5) OVER (PARTITION BY rr.race_id) AS weight_diff

FROM
  `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr;

-- Usage example:
-- SELECT *
-- FROM `horse-predictor-v2.horse_data_v2.records_context`;
