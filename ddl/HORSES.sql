CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.HORSES` (
  horse_id INT64,
  horse_name STRING,
  horse_country STRING,
  birth_year INT64,
  horse_sex STRING,
  breed STRING,
  mother_id INT64,
  father_id INT64,
  trainer_id INT64,
  breeder_id INT64,
  color_name_pl STRING,
  color_name_en STRING,
  polish_breeding BOOL,
  foreign_training BOOL,
  owner_name STRING
);