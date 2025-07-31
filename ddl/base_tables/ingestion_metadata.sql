-- ingestion_metadata.sql
-- This table tracks the last processed timestamp for each data prefix

CREATE TABLE `horse-predictor-v2.horse_data_v2.ingestion_metadata` (
  prefix              STRING NOT NULL,   -- e.g. 'horse_data/'
  last_processed_time TIMESTAMP NOT NULL  -- watermark of latest ingested file
)
PARTITION BY DATE(last_processed_time);
