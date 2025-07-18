CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.HORSES` (
  horse_id           INT64     OPTIONS(description="Primary key from /horse/{id}"),
  horse_name         STRING    OPTIONS(description="Name of the horse"),
  horse_country      STRING    OPTIONS(description="ISO-3 country code where the horse was bred"),
  birth_year         INT64     OPTIONS(description="Year the horse was born"),
  horse_sex          STRING    OPTIONS(description="Sex of the horse: MARE, GELDING, STALLION, etc."),
  breed              STRING    OPTIONS(description="Breed of the horse, e.g. ARABIAN"),
  mother_id          INT64     OPTIONS(description="Self-referential foreign key to mother horse_id"),
  father_id          INT64     OPTIONS(description="Self-referential foreign key to father horse_id"),
  trainer_id         INT64     OPTIONS(description="Foreign key to TRAINERS table"),
  breeder_id         INT64     OPTIONS(description="Foreign key to BREEDERS table"),
  color_name_pl      STRING    OPTIONS(description="Color name in Polish"),
  color_name_en      STRING    OPTIONS(description="Color name in English"),
  polish_breeding    BOOL      OPTIONS(description="True if bred in Poland"),
  foreign_training   BOOL      OPTIONS(description="True if trained or raced abroad"),
  owner_name         STRING    OPTIONS(description="Primary owner’s name"),
  created_date       TIMESTAMP OPTIONS(description="Timestamp when record was first created"),
  last_updated_date  TIMESTAMP OPTIONS(description="Timestamp when record was last updated")
);