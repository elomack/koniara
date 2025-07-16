CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.RACE_RECORDS` (
  race_record_id   INT64,   -- the record’s own ID
  race_id          INT64,
  horse_id         INT64,
  start_order      INT64,   -- order
  finish_place     INT64,   -- place
  jockey_weight_kg FLOAT64,
  prize_amount     FLOAT64,
  prize_currency   STRING,  -- inherit from RACES.currency_code
  jockey_id        INT64,
  trainer_id       INT64
);