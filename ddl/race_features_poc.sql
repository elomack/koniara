-- race_features_poc view with extended features for full-placement POC
CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.race_features_poc` AS

WITH base AS (
  SELECT
    -- Ids
    rr.horse_id,
    rr.race_id,
    rr.jockey_id,
    rr.trainer_id,
    h.breeder_id,

    -- True label (all finish places)
    rr.finish_place                AS finish_place,

    -- Raw race distance & derived bucket
    r.track_distance_m             AS distance_m,
    CASE
      WHEN r.track_distance_m > 2900 THEN 'very_long'
      WHEN r.track_distance_m BETWEEN 2300 AND 2900 THEN 'long'
      WHEN r.track_distance_m BETWEEN 1700 AND 2299 THEN 'medium'
      WHEN r.track_distance_m BETWEEN 1100 AND 1699 THEN 'short'
      WHEN r.track_distance_m < 1100  THEN 'very_short'
      ELSE 'unknown'
    END                             AS distance_bucket,

    -- Temperature numeric & bucket
    -- Clamp unrealistic temps (<-50 or >50) to NULL
    CASE
      WHEN SAFE_CAST(r.temperature_c AS FLOAT64) BETWEEN -50 AND 50 THEN SAFE_CAST(r.temperature_c AS FLOAT64)
      ELSE NULL
    END AS temp_c,
    CASE
      WHEN r.temperature_c > 30             THEN 'temp_gt_30'
      WHEN r.temperature_c BETWEEN 23 AND 30 THEN 'temp_23_30'
      WHEN r.temperature_c BETWEEN 15 AND 22 THEN 'temp_15_22'
      WHEN r.temperature_c BETWEEN 7 AND 14  THEN 'temp_7_14'
      WHEN r.temperature_c <= 6             THEN 'temp_le_6'
      ELSE 'temp_unknown'
    END                             AS temp_bucket,

    -- Weather flags
    CASE WHEN r.weather IN (
      'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
      'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
      'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
      'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
    ) THEN 1 ELSE 0 END             AS is_rainy,
    CASE WHEN r.weather IN ('pogodnie','słonecznie','pogodnie, słonecznie') THEN 1 ELSE 0 END AS is_sunny,
    CASE WHEN r.weather = 'upalnie'        THEN 1 ELSE 0 END AS is_hot,
    CASE WHEN r.weather IN (
      'pochmurno','pochm z przej','pochmurno z przejaśnieniami',
      'pochmurno z przejasnieniami','pochmurno z przej'
    ) THEN 1 ELSE 0 END             AS is_cloudy,
    CASE WHEN r.weather IN ('mgliście','mgła','mglisto') THEN 1 ELSE 0 END AS is_foggy,

        -- Track surface normalization (common Polish track types)
    CASE
      WHEN r.track_type IN (
        'lekko elastyczny','elastyczny','mocno elastyczny',
        'lekki','dobry','miękki','ciężki','twardy'
      ) THEN r.track_type
      ELSE 'other'
    END                             AS track_surface_cat,
    r.category_id,
    r.race_group,
    r.subtype,

    -- Horse career metrics
    hc.race_count                  AS horse_race_count,
    SAFE_DIVIDE(hc.race_won_count, hc.race_count) AS horse_win_pct,
    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tot
       WHERE rr_tot.horse_id = rr.horse_id)       AS career_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_tot2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tot2
      WHERE rr_tot2.horse_id = rr.horse_id)        AS career_win_pct,
    (SELECT COALESCE(SUM(SAFE_CAST(rr_prize.prize_amount AS FLOAT64)), 0)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prize
      WHERE rr_prize.horse_id = rr.horse_id)        AS career_prize_sum,
        -- Prize bucket based on career_prize_sum
    CASE
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) = 0 THEN 'p_0'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 1200 THEN 'p_1_1200'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 1800 THEN 'p_1201_1800'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 2400 THEN 'p_1801_2400'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 3200 THEN 'p_2401_3200'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 7000 THEN 'p_3201_7000'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 12000 THEN 'p_7001_12000'
      WHEN (SELECT COALESCE(SUM(SAFE_CAST(rr_prz.prize_amount AS FLOAT64)), 0) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prz WHERE rr_prz.horse_id = rr.horse_id) <= 16000 THEN 'p_12001_16000'
      ELSE 'p_16000+'
    END AS prize_amount_bucket,

    -- Record-level metrics
    CASE WHEN rr.jockey_weight_kg <= 100 THEN rr.jockey_weight_kg ELSE NULL END AS jockey_weight_kg,
    SAFE_CAST(rr.prize_amount AS FLOAT64)   AS prize_amount,

        -- Rest & form
    DATE_DIFF(r.race_date,
      LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) AS rest_days,
    CASE
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 7 THEN 'rest_lt_7'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 14 THEN 'rest_7_14'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 21 THEN 'rest_14_21'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 28 THEN 'rest_21_28'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 60 THEN 'rest_28_60'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) < 180 THEN 'rest_60_180'
      WHEN DATE_DIFF(r.race_date,
        LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date), DAY) <= 365 THEN 'rest_180_365'
      ELSE 'other'
    END                                 AS rest_bucket,
    CASE WHEN LAG(rr.finish_place) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date) = 1 THEN 1 ELSE 0 END AS won_last_race,

    -- Expanded form metrics using top-N subqueries with joined race dates
    (SELECT SAFE_DIVIDE(SUM(CASE WHEN sub.finish_place=1 THEN 1 ELSE 0 END), 2)
       FROM (
         SELECT rr2.finish_place
         FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr2
         JOIN `horse-predictor-v2.horse_data_v2.RACES` r2 ON rr2.race_id = r2.race_id
         WHERE rr2.horse_id = rr.horse_id
           AND r2.race_date < r.race_date
         ORDER BY r2.race_date DESC
         LIMIT 2
       ) AS sub
    )                                                         AS win_rate_last_2,
    (SELECT SAFE_DIVIDE(SUM(CASE WHEN sub.finish_place=1 THEN 1 ELSE 0 END), 7)
       FROM (
         SELECT rr7.finish_place
         FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr7
         JOIN `horse-predictor-v2.horse_data_v2.RACES` r7 ON rr7.race_id = r7.race_id
         WHERE rr7.horse_id = rr.horse_id
           AND r7.race_date < r.race_date
         ORDER BY r7.race_date DESC
         LIMIT 7
       ) AS sub
    )                                                         AS win_rate_last_7,
    (SELECT SAFE_DIVIDE(SUM(CASE WHEN sub.finish_place>1 THEN 1 ELSE 0 END), 3)
       FROM (
         SELECT rr3.finish_place
         FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr3
         JOIN `horse-predictor-v2.horse_data_v2.RACES` r3 ON rr3.race_id = r3.race_id
         WHERE rr3.horse_id = rr.horse_id
           AND r3.race_date < r.race_date
         ORDER BY r3.race_date DESC
         LIMIT 3
       ) AS sub
    )                                                         AS loss_rate_last_3,
    (SELECT SAFE_DIVIDE(SUM(CASE WHEN sub.finish_place>1 THEN 1 ELSE 0 END), 5)
       FROM (
         SELECT rr5.finish_place
         FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr5
         JOIN `horse-predictor-v2.horse_data_v2.RACES` r5 ON rr5.race_id = r5.race_id
         WHERE rr5.horse_id = rr.horse_id
           AND r5.race_date < r.race_date
         ORDER BY r5.race_date DESC
         LIMIT 5
       ) AS sub
    )                                                         AS loss_rate_last_5,

    -- Entity metrics
    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_jcount
      WHERE rr_jcount.jockey_id = rr.jockey_id)                     AS jockey_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_j.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_j
      WHERE rr_j.jockey_id = rr.jockey_id)                          AS jockey_win_pct,

    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tcount
      WHERE rr_tcount.trainer_id = rr.trainer_id)                   AS trainer_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_t.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t
      WHERE rr_t.trainer_id = rr.trainer_id)                        AS trainer_win_pct,

    (SELECT COUNT(*)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_bcount
       JOIN `horse-predictor-v2.horse_data_v2.HORSES` h_b ON rr_bcount.horse_id = h_b.horse_id
      WHERE h_b.breeder_id = h.breeder_id)                          AS breeder_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_b.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_b
       JOIN `horse-predictor-v2.horse_data_v2.HORSES` h_b2 ON rr_b.horse_id = h_b2.horse_id
      WHERE h_b2.breeder_id = h.breeder_id)                         AS breeder_win_pct,

    -- Synergy metrics
    (SELECT SAFE_DIVIDE(COUNTIF(rr_hj.finish_place<=3), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_hj
      WHERE rr_hj.horse_id = rr.horse_id AND rr_hj.jockey_id = rr.jockey_id
    )                                                             AS hj_podium_rate,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_ht.finish_place<=3), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_ht
      WHERE rr_ht.horse_id = rr.horse_id AND rr_ht.trainer_id = rr.trainer_id
    )                                                             AS ht_podium_rate,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_hb.finish_place<=3), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_hb
       JOIN `horse-predictor-v2.horse_data_v2.HORSES` hbb ON rr_hb.horse_id = hbb.horse_id
      WHERE rr_hb.horse_id = rr.horse_id AND hbb.breeder_id = h.breeder_id
    )                                                             AS hb_podium_rate,

    -- Distance specialization
    (SELECT SAFE_DIVIDE(COUNTIF(rr_d.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_d
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_d ON rr_d.race_id = r_d.race_id
      WHERE rr_d.horse_id = rr.horse_id AND r_d.track_distance_m = 1600
    )                                                             AS win_pct_1600,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_d2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_d2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_d2 ON rr_d2.race_id = r_d2.race_id
      WHERE rr_d2.horse_id = rr.horse_id AND r_d2.track_distance_m = 2000
    )                                                             AS win_pct_2000,

    -- Weather sensitivity & temperature effect
    ((SELECT SAFE_DIVIDE(COUNTIF(rr_w.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_w
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_w ON rr_w.race_id = r_w.race_id
      WHERE rr_w.horse_id = rr.horse_id AND r_w.weather IN (
        'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
        'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
        'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
        'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
      )) -
     (SELECT SAFE_DIVIDE(COUNTIF(rr_w2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_w2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_w2 ON rr_w2.race_id = r_w2.race_id
      WHERE rr_w2.horse_id = rr.horse_id AND r_w2.weather NOT IN (
        'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
        'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
        'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
        'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
      )))                                                        AS win_pct_delta_rain,
    ((SELECT AVG(rr_t2.finish_place)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_t2 ON rr_t2.race_id = r_t2.race_id
      WHERE rr_t2.horse_id = rr.horse_id AND r_t2.temperature_c > 20
    ) -
     (SELECT AVG(rr_t3.finish_place)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t3
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_t3 ON rr_t3.race_id = r_t3.race_id
      WHERE rr_t3.horse_id = rr.horse_id AND r_t3.temperature_c <= 20
    ))                                                           AS avg_place_delta_temp

  FROM
    `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr
  JOIN `horse-predictor-v2.horse_data_v2.RACES` r  ON rr.race_id = r.race_id
  JOIN `horse-predictor-v2.horse_data_v2.HORSE_CAREERS` hc ON rr.horse_id = hc.horse_id
     AND EXTRACT(YEAR FROM SAFE_CAST(r.race_date AS DATE)) = hc.race_year
  LEFT JOIN `horse-predictor-v2.horse_data_v2.HORSES` h  ON rr.horse_id = h.horse_id
)
SELECT *
FROM base;
