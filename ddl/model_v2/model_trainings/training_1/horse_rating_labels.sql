-- train_horse_rating.sql
-- Train, evaluate, and then persist evaluation metrics for future comparison

-- 0) Ensure the history table exists to store model evaluation metrics
CREATE TABLE IF NOT EXISTS
  `horse-predictor-v2.horse_data_v2.model_eval_history` (
    model_name STRING,
    eval_timestamp DATETIME,
    mean_absolute_error FLOAT64,
    mean_squared_error FLOAT64,
    mean_squared_log_error FLOAT64,
    median_absolute_error FLOAT64,
    r2_score FLOAT64,
    explained_variance FLOAT64
  );

-- 1) Train the global horse rating regression model with increased iterations
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
  MAX_ITERATIONS           = 200                 -- Increase iterations for convergence
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

-- 2) Evaluate the newly trained regression model
CREATE OR REPLACE TABLE
  `horse-predictor-v2.horse_data_v2.latest_model_eval` AS
SELECT
  'global_rating' AS model_name,
  CURRENT_DATETIME() AS eval_timestamp,
  * EXCEPT(model_name)
FROM
  ML.EVALUATE(
    MODEL `horse-predictor-v2.horse_data_v2.train_horse_rating`
  );

-- 3) Persist evaluation metrics for this run into history
INSERT INTO
  `horse-predictor-v2.horse_data_v2.model_eval_history`
SELECT
  model_name,
  eval_timestamp,
  mean_absolute_error,
  mean_squared_error,
  mean_squared_log_error,
  median_absolute_error,
  r2_score,
  explained_variance
FROM
  `horse-predictor-v2.horse_data_v2.latest_model_eval`;
