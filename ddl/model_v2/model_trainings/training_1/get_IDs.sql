-- SQL query to map race entries to their IDs in BigQuery, with robust matching

-- 1) Define your race entries with the scraped columns
WITH race_entries AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS starting_pos,  'Sputnik'                 AS horse_name, 'Grzybowski K.'        AS jockey_last_name, 'Wnorowski M.'        AS trainer_last_name, 56.5 AS jockey_weight),
    STRUCT(2 AS starting_pos,  'Fantagram'               AS horse_name, 'Kamińska K.'          AS jockey_last_name, 'Rogowski K.'         AS trainer_last_name, 51   AS jockey_weight),
    STRUCT(3 AS starting_pos,  'Farhat'                AS horse_name, 'Marat Uulu B.'        AS jockey_last_name, 'Wyrzyk A.'           AS trainer_last_name, 65   AS jockey_weight),
    STRUCT(4 AS starting_pos,  'Dioriska'                AS horse_name, 'Urmatbek Uulu S.'     AS jockey_last_name, 'Borkowski M.'        AS trainer_last_name, 62   AS jockey_weight),
    STRUCT(5 AS starting_pos,  'Menwaal Al Shahania'     AS horse_name, 'Serafin A.'           AS jockey_last_name, 'Laskowski A.'        AS trainer_last_name, 62.5 AS jockey_weight),
    STRUCT(6 AS starting_pos,  'Astonishing Grace'       AS horse_name, 'Zamudin Uulu E.'      AS jockey_last_name, 'Domańska J.'         AS trainer_last_name, 60   AS jockey_weight),
    STRUCT(7 AS starting_pos,  'Legende du Pouy'         AS horse_name, 'Stadnicka A.'         AS jockey_last_name, 'Pawlak C.'           AS trainer_last_name, 61   AS jockey_weight)
  ])
),

-- 2) Join to master tables to retrieve IDs, using LOWER for case-insensitive matching
joined AS (
  SELECT
    re.starting_pos,
    re.horse_name,
    re.jockey_last_name,
    re.trainer_last_name,
    re.jockey_weight,
    h.horse_id,
    j.jockey_id,
    t.trainer_id
  FROM race_entries AS re

  -- match on horse name (case-insensitive)
  LEFT JOIN `horse_data_v2.HORSES` AS h
    ON LOWER(TRIM(h.horse_name)) = LOWER(TRIM(re.horse_name))

  -- match on jockey last name (case-insensitive)
  LEFT JOIN `horse_data_v2.JOCKEYS` AS j
    ON LOWER(TRIM(j.last_name)) = LOWER(TRIM(re.jockey_last_name))

  -- match on trainer last name (case-insensitive)
  LEFT JOIN `horse_data_v2.TRAINERS` AS t
    ON LOWER(TRIM(t.last_name)) = LOWER(TRIM(re.trainer_last_name))
)

-- 3) Select final columns and check for unmatched rows
SELECT
  starting_pos     AS starting_position,
  horse_name,
  jockey_last_name,
  trainer_last_name,
  horse_id,
  trainer_id,
  jockey_id,
  jockey_weight
FROM joined
ORDER BY starting_pos;
