CREATE OR REPLACE TABLE `horse-predictor-v2.horse_data_v2.BREEDERS` (
  breeder_id         INT64     OPTIONS(description="Primary key from breeder.id"),
  name               STRING    OPTIONS(description="Name of the breeder"),
  city               STRING    OPTIONS(description="City of the breeder"),
  created_date       TIMESTAMP OPTIONS(description="Timestamp when record was first created"),
  last_updated_date  TIMESTAMP OPTIONS(description="Timestamp when record was last updated")
);