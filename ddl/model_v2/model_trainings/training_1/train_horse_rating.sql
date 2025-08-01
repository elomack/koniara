-- train_horse_rating.sql
-- Train the global horse rating regression model, filtering out null labels

CREATE OR REPLACE MODEL
  `horse-predictor-v2.horse_data_v2.train_horse_rating`
OPTIONS(
  MODEL_TYPE               = 'linear_reg',       -- Regression on 0–100 score
  INPUT_LABEL_COLS         = ['rating_score'],   -- Label column
  DATA_SPLIT_METHOD        = 'RANDOM',           -- Random train/val split
  DATA_SPLIT_EVAL_FRACTION = 0.15,               -- 15% validation
  L2_REG                   = 1.0,                -- L2 regularization
  LEARN_RATE_STRATEGY      = 'constant',         -- Fixed learning rate
  LEARN_RATE               = 0.1,                -- Step size for gradient descent
  MAX_ITERATIONS           = 50                  -- Max number of iterations
) AS
SELECT
  hf.*, 
  lr.rating_score
FROM
  `horse-predictor-v2.horse_data_v2.horse_features`(DATE '2025-07-31') AS hf
JOIN
  `horse-predictor-v2.horse_data_v2.horse_rating_labels` AS lr
  ON CAST(hf.horse_id AS STRING) = lr.horse_id
     AND lr.snapshot_date = DATE '2025-07-31'
WHERE
  lr.rating_score IS NOT NULL;
