-- 1) Train v2 softmax model, using the rebuilt race_features view
CREATE OR REPLACE MODEL
  `horse-predictor-v2.horse_data_v2.train_race_prediction_v1`
OPTIONS(
  MODEL_TYPE               = 'logistic_reg',      -- Softmax multiclass
  INPUT_LABEL_COLS         = ['finish_place'],    -- Label
  DATA_SPLIT_METHOD        = 'RANDOM',            -- Random 70/15/15 split
  DATA_SPLIT_EVAL_FRACTION = 0.15,                -- 15% validation
  L2_REG                   = 1.0,                 -- L2 regularization
  LEARN_RATE_STRATEGY      = 'constant',          -- Fixed learning rate
  LEARN_RATE               = 0.05,                -- Step size
  MAX_ITERATIONS           = 30                   -- Iteration cap
) AS
SELECT
  * EXCEPT(finish_place),
  finish_place
FROM
  `horse-predictor-v2.horse_data_v2.race_features`
WHERE
  finish_place IS NOT NULL;  -- drop any unlabeled rows

-- 2) Evaluate the newly trained race‐prediction v2 model
CREATE OR REPLACE TABLE
  `horse-predictor-v2.horse_data_v2.latest_race_eval_v1` AS
SELECT
  LOG_LOSS    AS log_loss,
  ACCURACY    AS accuracy,
  PRECISION   AS precision,
  RECALL      AS recall,
  F1_SCORE    AS f1_score,
  CURRENT_DATETIME() AS eval_timestamp
FROM
  ML.EVALUATE(
    MODEL `horse-predictor-v2.horse_data_v2.train_race_prediction_v1`
  );

-- 3) Insert v2 metrics into the model_registry
INSERT INTO
  `horse-predictor-v2.horse_data_v2.model_registry`
(
  model_name,
  run_timestamp,
  model_type,
  label_column,
  training_window,
  feature_snapshot,
  log_loss,
  accuracy,
  precision,
  recall,
  f1_score,
  description
)
SELECT
  'race_prediction_v1'                        AS model_name,
  eval_timestamp                              AS run_timestamp,
  'logistic_reg'                              AS model_type,
  'finish_place'                              AS label_column,
  'All races, random 70/15/15 split'          AS training_window,
  NULL                                        AS feature_snapshot,
  log_loss,
  accuracy,
  precision,
  recall,
  f1_score,
  'v1: dropped NULL labels, removed payouts, explicit defaults for recent-form nulls' AS description
FROM
  `horse-predictor-v2.horse_data_v2.latest_race_eval_v1`;
