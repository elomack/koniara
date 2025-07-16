CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.RACES` (
  race_id            INT64,
  race_number        INT64,
  race_name          STRING,
  race_date          TIMESTAMP,
  currency_code      STRING,   -- ISO-3 code
  currency_symbol    STRING,
  duration_ms        INT64,
  track_distance_m   INT64,
  temperature_c      FLOAT,
  weather            STRING,
  group              STRING,
  subtype            STRING,
  category_id        INT64,
  category_breed     STRING,
  category_name      STRING,
  country_code       STRING,   -- ISO-3 code
  city_name          STRING,
  track_type         STRING,
  video_url          STRING,
  race_rules         STRING,   -- fullConditions
  payments           STRING,
  race_style         STRING    -- style.name
);