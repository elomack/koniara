-- 2) Train Per-Race Softmax Probability Model
CREATE OR REPLACE MODEL
  `horse-predictor-v2.horse_data_v2.train_race_prediction`
OPTIONS(
  MODEL_TYPE              = 'logistic_reg',      -- Multiclass softmax logistic regression
  INPUT_LABEL_COLS        = ['finish_place'],    -- Label: ordinal finish place
  DATA_SPLIT_METHOD       = 'RANDOM',            -- Random train/val split
  DATA_SPLIT_EVAL_FRACTION= 0.15,                 -- 15% validation
  L2_REG                  = 1.0,                 -- L2 regularization
  LEARN_RATE_STRATEGY     = 'constant',          -- Fixed learning rate
  LEARN_RATE              = 0.05,                -- Step size for gradient descent
  MAX_ITERATIONS          = 30                  -- Max number of iterations
) AS
SELECT
  * EXCEPT(finish_place),       -- all feature columns except the label
  finish_place                  -- ground-truth label
FROM
  `horse-predictor-v2.horse_data_v2.race_features`;