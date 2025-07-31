CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.TRAINERS` (
  trainer_id         INT64     OPTIONS(description="Primary key from trainer.id"),
  first_name         STRING    OPTIONS(description="Trainer’s first name"),
  last_name          STRING    OPTIONS(description="Trainer’s last name"),
  licence_country    STRING    OPTIONS(description="ISO-3 country code of licence issuer"),
  created_date       TIMESTAMP OPTIONS(description="Timestamp when record was first created"),
  last_updated_date  TIMESTAMP OPTIONS(description="Timestamp when record was last updated")
);