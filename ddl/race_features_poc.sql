-- race_features_poc view definition with extended POC features and data quality fixes
CREATE OR REPLACE VIEW
  `horse-predictor-v2.horse_data_v2.race_features_poc` AS

WITH base AS (
  SELECT
    rr.horse_id,
    rr.race_id,
    rr.jockey_id,
    rr.trainer_id,
    h.breeder_id,
    rr.finish_place        AS finish_place,

    -- Race features
    r.track_distance_m      AS distance_m,

    -- Temperature as numeric and bucketed
    SAFE_CAST(r.temperature_c AS FLOAT64)    AS temp_c,
    CASE
      WHEN r.temperature_c > 30              THEN 'temp_gt_30'
      WHEN r.temperature_c BETWEEN 23 AND 30  THEN 'temp_23_30'
      WHEN r.temperature_c BETWEEN 15 AND 22  THEN 'temp_15_22'
      WHEN r.temperature_c BETWEEN 7 AND 14   THEN 'temp_7_14'
      WHEN r.temperature_c <= 6               THEN 'temp_le_6'
      ELSE 'temp_unknown'
    END                         AS temp_bucket,

    -- Weather flags (Polish)
    CASE WHEN r.weather IN (
      'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
      'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
      'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
      'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
    ) THEN 1 ELSE 0 END        AS is_rainy,
    CASE WHEN r.weather IN ('pogodnie','słonecznie','pogodnie, słonecznie') THEN 1 ELSE 0 END AS is_sunny,
    CASE WHEN r.weather = 'upalnie'       THEN 1 ELSE 0 END AS is_hot,
    CASE WHEN r.weather IN (
      'pochmurno','pochm z przej','pochmurno z przejaśnieniami',
      'pochmurno z przejasnieniami','pochmurno z przej'
    ) THEN 1 ELSE 0 END        AS is_cloudy,
    CASE WHEN r.weather IN ('mgliście','mgła','mglisto') THEN 1 ELSE 0 END AS is_foggy,

    -- Track metadata
    r.track_type               AS track_type_cat,
    r.category_id,
    r.race_group,
    r.subtype,

    -- Horse career (yearly vs. all-time)
    hc.race_count              AS horse_race_count,   -- races in this calendar year
    SAFE_DIVIDE(hc.race_won_count, hc.race_count) AS horse_win_pct,
    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tot WHERE rr_tot.horse_id = rr.horse_id) AS career_race_count,  -- total lifetime races

    -- Career prize and win totals
    (SELECT SAFE_DIVIDE(COUNTIF(rr_tot2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tot2
      WHERE rr_tot2.horse_id = rr.horse_id
    )                                         AS career_win_pct,
    (SELECT COALESCE(SUM(rr_prize.prize_amount), 0)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_prize
      WHERE rr_prize.horse_id = rr.horse_id
    )                                         AS career_prize_sum,

    -- Record-level metrics
    CASE WHEN rr.jockey_weight_kg <= 100 THEN rr.jockey_weight_kg ELSE NULL END AS jockey_weight_kg,
    rr.prize_amount                           AS prize_amount,
    -- Flag extreme prize values for debugging
    CASE WHEN rr.prize_amount > 10000000 THEN 1 ELSE 0 END AS is_prize_extreme,

    -- Rest & form
    DATE_DIFF(r.race_date,
      LAG(r.race_date) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date),
      DAY)                                  AS rest_days,
    CASE WHEN LAG(rr.finish_place) OVER (PARTITION BY rr.horse_id ORDER BY r.race_date) = 1 THEN 1 ELSE 0 END AS won_last_race,

    -- Jockey metrics & counts
    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_jcount WHERE rr_jcount.jockey_id = rr.jockey_id) AS jockey_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_j.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_j
      WHERE rr_j.jockey_id = rr.jockey_id
    )                                     AS jockey_win_pct,

    -- Trainer metrics & counts
    (SELECT COUNT(*) FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_tcount WHERE rr_tcount.trainer_id = rr.trainer_id) AS trainer_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_t.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t
      WHERE rr_t.trainer_id = rr.trainer_id
    )                                     AS trainer_win_pct,

    -- Breeder metrics & counts
    (SELECT COUNT(*)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_bcount
       JOIN `horse-predictor-v2.horse_data_v2.HORSES` h_b ON rr_bcount.horse_id = h_b.horse_id
      WHERE h_b.breeder_id = h.breeder_id
    )                                     AS breeder_race_count,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_b.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_b
       JOIN `horse-predictor-v2.horse_data_v2.HORSES` h_b2 ON rr_b.horse_id = h_b2.horse_id
      WHERE h_b2.breeder_id = h.breeder_id
    )                                     AS breeder_win_pct,

    -- Horse-Jockey & Horse-Trainer synergy
    (SELECT SAFE_DIVIDE(COUNTIF(rr_hj.finish_place<=3), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_hj
      WHERE rr_hj.horse_id = rr.horse_id AND rr_hj.jockey_id = rr.jockey_id
    )                                     AS hj_podium_rate,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_ht.finish_place<=3), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_ht
      WHERE rr_ht.horse_id = rr.horse_id AND rr_ht.trainer_id = rr.trainer_id
    )                                     AS ht_podium_rate,

    -- Distance specialization
    (SELECT SAFE_DIVIDE(COUNTIF(rr_d.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_d
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_d ON rr_d.race_id = r_d.race_id
      WHERE rr_d.horse_id = rr.horse_id AND r_d.track_distance_m = 1600
    )                                     AS win_pct_1600,
    (SELECT SAFE_DIVIDE(COUNTIF(rr_d2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_d2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_d2 ON rr_d2.race_id = r_d2.race_id
      WHERE rr_d2.horse_id = rr.horse_id AND r_d2.track_distance_m = 2000
    )                                     AS win_pct_2000,

    -- Weather sensitivity
    ((SELECT SAFE_DIVIDE(COUNTIF(rr_w.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_w
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_w ON rr_w.race_id = r_w.race_id
      WHERE rr_w.horse_id = rr.horse_id AND r_w.weather IN (
        'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
        'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
        'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
        'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
      )
    ) -
    (SELECT SAFE_DIVIDE(COUNTIF(rr_w2.finish_place=1), COUNT(*))
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_w2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_w2 ON rr_w2.race_id = r_w2.race_id
      WHERE rr_w2.horse_id = rr.horse_id AND r_w2.weather NOT IN (
        'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
        'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
        'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
        'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
      )
    ))                                   AS win_pct_delta_rain,

    -- Temperature sensitivity
    ((SELECT AVG(rr_t2.finish_place)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t2
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_t2 ON rr_t2.race_id = r_t2.race_id
      WHERE rr_t2.horse_id = rr.horse_id AND r_t2.temperature_c > 20
    ) -
    (SELECT AVG(rr_t3.finish_place)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_t3
       JOIN `horse-predictor-v2.horse_data_v2.RACES` r_t3 ON rr_t3.race_id = r_t3.race_id
      WHERE rr_t3.horse_id = rr.horse_id AND r_t3.temperature_c <= 20
    ))                                   AS avg_place_delta_temp,

    -- Pedigree influence
    h.father_id            AS father_id,
    -- Father's own race counts
    (SELECT COUNT(*)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_f
      WHERE rr_f.horse_id = h.father_id
    )                                     AS father_race_count,
    (SELECT COUNTIF(rr_f2.finish_place=1)
       FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_f2
      WHERE rr_f2.horse_id = h.father_id
    )                                     AS father_win_count,
    -- Father's win rate
    SAFE_DIVIDE(
      (SELECT COUNTIF(rr_f2.finish_place=1)
         FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_f2
        WHERE rr_f2.horse_id = h.father_id
      ),
      NULLIF(
        (SELECT COUNT(*)
           FROM `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr_f
          WHERE rr_f.horse_id = h.father_id
        ), 0
      )
    )                                     AS father_win_pct

  FROM
    `horse-predictor-v2.horse_data_v2.RACE_RECORDS` rr
  JOIN `horse-predictor-v2.horse_data_v2.RACES`        r  ON rr.race_id = r.race_id
  JOIN `horse-predictor-v2.horse_data_v2.HORSE_CAREERS` hc ON rr.horse_id = hc.horse_id
     AND EXTRACT(YEAR FROM SAFE_CAST(r.race_date AS DATE)) = hc.race_year
  LEFT JOIN `horse-predictor-v2.horse_data_v2.HORSES`  h  ON rr.horse_id = h.horse_id
  WHERE rr.finish_place BETWEEN 1 AND 4
)
SELECT * FROM base;
