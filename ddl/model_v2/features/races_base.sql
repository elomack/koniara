-- features/races_base.sql
-- View for static race-level features: distance (continuous + buckets), temperature, style, classification, breed, location, track type

CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.races_base` AS
SELECT
  race_id,

  -- Continuous distance
  track_distance_m AS distance_m,
  -- Distance buckets
  CASE WHEN track_distance_m <= 1200 THEN TRUE ELSE FALSE END AS dist_l1200,
  CASE WHEN track_distance_m BETWEEN 1201 AND 1799 THEN TRUE ELSE FALSE END AS dist_1200_1799,
  CASE WHEN track_distance_m BETWEEN 1800 AND 2399 THEN TRUE ELSE FALSE END AS dist_1800_2399,
  CASE WHEN track_distance_m BETWEEN 2400 AND 3000 THEN TRUE ELSE FALSE END AS dist_2400_3000,
  CASE WHEN track_distance_m > 3000 THEN TRUE ELSE FALSE END AS dist_m3000,

  -- Temperature continuous
  temperature_c,

    -- Weather flags
  CASE WHEN weather IN (
    'pochmurno, przel deszcz','pochmurno, deszcz','pochm z przej, przel deszcz',
    'pochmurno, przelotny deszcz','deszcz','pochmurno, opady deszczu',
    'pochmurno, przelotne opady deszczu','pochmurno, przel opady',
    'pochmurno z przejaśnieniami, przelotny deszcz','burzowo','deszczowo'
  ) THEN TRUE ELSE FALSE END AS is_rainy,
  CASE WHEN weather IN (
    'pogodnie','słonecznie','pogodnie, słonecznie'
  ) THEN TRUE ELSE FALSE END AS is_sunny,
  CASE WHEN weather = 'upalnie' THEN TRUE ELSE FALSE END AS is_hot,
  CASE WHEN weather IN (
    'pochmurno','pochm z przej','pochmurno z przejaśnieniami',
    'pochmurno z przejasnieniami','pochmurno z przej'
  ) THEN TRUE ELSE FALSE END AS is_cloudy,
  CASE WHEN weather IN (
    'mgliście','mgła','mglisto'
  ) THEN TRUE ELSE FALSE END AS is_foggy,

  -- Style ease score as defined
  CASE race_style
    WHEN 'w walce'         THEN 1
    WHEN 'po walce'        THEN 2
    WHEN 'silnie wysyłany' THEN 3
    WHEN 'wysyłany'        THEN 4
    WHEN 'pewnie'          THEN 5
    WHEN 'łatwo'           THEN 6
    WHEN 'lekko'           THEN 6
    WHEN 'bardzo łatwo'    THEN 7
    WHEN 'dowolnie'        THEN 8
    ELSE NULL
  END AS style_ease_score,

  -- Race Group classification
  CASE WHEN race_group = 'GROUP_I'       THEN TRUE ELSE FALSE END AS group_I,
  CASE WHEN race_group = 'GROUP_II'      THEN TRUE ELSE FALSE END AS group_II,
  CASE WHEN race_group = 'GROUP_III'     THEN TRUE ELSE FALSE END AS group_III,
  CASE WHEN race_group = 'GROUP_IV'      THEN TRUE ELSE FALSE END AS group_IV,
  CASE WHEN race_group = 'GROUP_NONE'    THEN TRUE ELSE FALSE END AS group_NONE,
  CASE WHEN race_group = 'SLED'         THEN TRUE ELSE FALSE END AS group_SLED,
  CASE WHEN race_group = 'HURDLE'       THEN TRUE ELSE FALSE END AS group_HURDLE,
  CASE WHEN race_group = 'STEEPLECHASE' THEN TRUE ELSE FALSE END AS group_STEEPLECHASE,
  CASE WHEN race_group = 'TRIAL'        THEN TRUE ELSE FALSE END AS group_TRIAL,

  -- Breed categories
  CASE WHEN category_breed = 'THOROUGHBRED'   THEN TRUE ELSE FALSE END AS breed_thoroughbred,
  CASE WHEN category_breed = 'ARABIAN'        THEN TRUE ELSE FALSE END AS breed_arabian,
  CASE WHEN category_breed = 'STANDARDBRED'   THEN TRUE ELSE FALSE END AS breed_standardbred,
  CASE WHEN category_breed = 'ANGLO_ARABIAN'  THEN TRUE ELSE FALSE END AS breed_anglo_arabian,

  -- Location flags
  CASE WHEN country_code = 'PL' THEN TRUE ELSE FALSE END AS country_PL,
  CASE WHEN city_name = 'Warsaw' THEN TRUE ELSE FALSE END AS city_Warsaw,

  -- Track type one-hot flags
  CASE WHEN track_type = 'lekko elastyczny'     THEN TRUE ELSE FALSE END AS surface_lekko_elastyczny,
  CASE WHEN track_type = 'elastyczny'           THEN TRUE ELSE FALSE END AS surface_elastyczny,
  CASE WHEN track_type = 'mocno elastyczny'     THEN TRUE ELSE FALSE END AS surface_mocno_elastyczny,
  CASE WHEN track_type = 'lekki'                THEN TRUE ELSE FALSE END AS surface_lekki,
  CASE WHEN track_type = 'dobry'                THEN TRUE ELSE FALSE END AS surface_dobry,
  CASE WHEN track_type = 'miękki'               THEN TRUE ELSE FALSE END AS surface_miekki,
  CASE WHEN track_type = 'ciężki'               THEN TRUE ELSE FALSE END AS surface_ciezki

FROM `horse-predictor-v2.horse_data_v2.RACES`;

-- Usage example:
-- SELECT * FROM `horse-predictor-v2.horse_data_v2.races_base`;
