-- features/races_payouts.sql
-- View to parse and normalize payout signals from the raw RACES.payments string

CREATE OR REPLACE VIEW `horse-predictor-v2.horse_data_v2.races_payouts` AS
SELECT
  race_id,
  -- Win-bet payoff (ZWC): low→favorite, high→underdog
  SAFE_CAST(
    REPLACE(
      REGEXP_EXTRACT(payments, r'ZWC\s*([0-9]+,[0-9]+)'), ',', '.'
    ) AS FLOAT64
  ) AS payout_zwc,

  -- Either-2 payoff (PDK): low→easy top2, high→difficult top2
  SAFE_CAST(
    REPLACE(
      REGEXP_EXTRACT(payments, r'PDK\s*([0-9]+,[0-9]+)'), ',', '.'
    ) AS FLOAT64
  ) AS payout_pdk,

    -- Exact-2 payoff (DWJ): low→easy top2, high→difficult top2
  SAFE_CAST(
    REPLACE(
      REGEXP_EXTRACT(payments, r'DWJ\s*([0-9]+,[0-9]+)'), ',', '.'
    ) AS FLOAT64
  ) AS payout_dwj,

  -- Exact-3 payoff (TRJ): low→easy top3, high→difficult top3
  SAFE_CAST(
    REPLACE(
      REGEXP_EXTRACT(payments, r'TRJ\s*([0-9]+,[0-9]+)'), ',', '.'
    ) AS FLOAT64
  ) AS payout_trj,

  -- Exact-4 payoff (TRJ): low→easy top4, high→difficult top4
  SAFE_CAST(
    REPLACE(
      REGEXP_EXTRACT(payments, r'CZW\s*([0-9]+,[0-9]+)'), ',', '.'
    ) AS FLOAT64
  ) AS payout_czw

FROM `horse-predictor-v2.horse_data_v2.RACES`;

-- Usage example:
-- SELECT *
-- FROM `horse-predictor-v2.horse_data_v2.races_payouts`;
