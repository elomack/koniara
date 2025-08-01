CREATE TABLE IF NOT EXISTS
  `horse-predictor-v2.horse_data_v2.model_registry` (
    model_name         STRING,       -- e.g. “global_horse_rating_v1”
    run_timestamp      DATETIME,     -- when training finished
    model_type         STRING,       -- “linear_reg” or “logistic_reg”
    label_column       STRING,       -- e.g. “rating_score”
    training_window    STRING,       -- free-text, e.g. “2018-2025 with 30y decay”
    feature_snapshot   DATE,         -- the HF TVF DATE argument
    mae                FLOAT64,
    rmse               FLOAT64,
    r2_score           FLOAT64,
    expl_variance      FLOAT64,
    log_loss        FLOAT64,
    accuracy        FLOAT64,
    top_k_accuracy  FLOAT64,
    description        STRING        -- notes on features / hyperparams
  );