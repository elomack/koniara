CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.HORSE_CAREERS` (
  horse_id           INT64     OPTIONS(description="Foreign key to HORSES.horse_id"),
  race_year          INT64     OPTIONS(description="Calendar year of races"),
  race_type          STRING    OPTIONS(description="Type of race, e.g. 'Sulki', 'Przeszkody'") ,
  horse_age          INT64     OPTIONS(description="Age of the horse in that racing year"),
  race_count         INT64     OPTIONS(description="Number of starts in that category/year"),
  race_won_count     INT64     OPTIONS(description="Number of wins"),
  race_prize_count   INT64     OPTIONS(description="Number of prize-earning races"),
  prize_amounts      STRING    OPTIONS(description="Comma-separated prize amounts, e.g. '27800,175'"),
  prize_currencies   STRING    OPTIONS(description="Comma-separated currency codes, e.g. 'PLN,EUR'"),
  created_date       TIMESTAMP OPTIONS(description="Timestamp when record was first created"),
  last_updated_date  TIMESTAMP OPTIONS(description="Timestamp when record was last updated")
);