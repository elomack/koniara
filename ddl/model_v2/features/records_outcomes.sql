-- features/records_outcomes.sql
-- View for per-record outcome flags: exact finish position and win/place/paid indicators

CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.records_outcomes` AS
SELECT
  rr.race_id,
  rr.horse_id,
  rr.finish_place,
  -- Win indicator
  CASE WHEN rr.finish_place = 1 THEN TRUE ELSE FALSE END AS is_win,
  -- Place indicator (top 3)
  CASE WHEN rr.finish_place <= 3 THEN TRUE ELSE FALSE END AS is_place,
  -- Paid indicator (top 5)
  CASE WHEN rr.finish_place <= 5 THEN TRUE ELSE FALSE END AS is_paid
FROM
  `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr;

-- Usage example:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.records_outcomes`;
