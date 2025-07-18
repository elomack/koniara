CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.RACE_RECORDS` (
  race_record_id     INT64     OPTIONS(description="Primary key from record.id"),
  race_id            INT64     OPTIONS(description="Foreign key to RACES.race_id"),
  horse_id           INT64     OPTIONS(description="Foreign key to HORSES.horse_id"),
  start_order        INT64     OPTIONS(description="Starting order position"),
  finish_place       INT64     OPTIONS(description="Finishing place position"),
  jockey_weight_kg   FLOAT64   OPTIONS(description="Weight of jockey in kg"),
  prize_amount       FLOAT64   OPTIONS(description="Prize awarded to this horse"),
  prize_currency     STRING    OPTIONS(description="Currency code for prize"),
  jockey_id          INT64     OPTIONS(description="Foreign key to JOCKEYS.jockey_id"),
  trainer_id         INT64     OPTIONS(description="Foreign key to TRAINERS.trainer_id"),
  created_date       TIMESTAMP OPTIONS(description="Timestamp when record was first created"),
  last_updated_date  TIMESTAMP OPTIONS(description="Timestamp when record was last updated")
);