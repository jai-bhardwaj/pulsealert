-- ============================================================================
-- PulseAlert — Confluent Cloud for Apache Flink SQL pipeline
-- Run each statement separately in the Flink SQL workspace (Environment ▸ Flink
-- ▸ Open SQL workspace), with your environment as the catalog and your cluster
-- as the database selected at the top of the workspace.
--
-- Topics written by others:
--   crypto.spot.raw  ← HTTP Source connector (Coinbase spot prices, JSON Schema)
--   alert_rules      ← Next.js app (user rules, JSON Schema)
-- Both appear automatically as tables because their schemas live in Schema Registry.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 0. Sanity check: the raw connector feed. Record shape is
--    { "data": { "amount": "64123.45", "base": "BTC", "currency": "USD" } }
--    (`data` is a reserved word, hence the backticks.)
-- ----------------------------------------------------------------------------
SELECT `data`.base AS symbol, `data`.amount, $rowtime
FROM `crypto.spot.raw`;


-- ----------------------------------------------------------------------------
-- 1. Clean, typed price ticks (Stream Governance: JSON Schema registered by Flink)
--    DISTRIBUTED BY puts `symbol` in the (Avro) Kafka key; 'value.fields-include'
--    = 'all' keeps it in the JSON value too, which is all the app decodes.
-- ----------------------------------------------------------------------------
CREATE TABLE price_ticks (
  symbol STRING NOT NULL,
  price  DOUBLE NOT NULL,
  ts     TIMESTAMP_LTZ(3) NOT NULL
) DISTRIBUTED BY HASH(symbol) INTO 1 BUCKETS
WITH (
  'value.format' = 'json-registry',
  'value.fields-include' = 'all'
);

INSERT INTO price_ticks
SELECT
  UPPER(`data`.base)              AS symbol,
  CAST(`data`.amount AS DOUBLE)   AS price,
  $rowtime                        AS ts
FROM `crypto.spot.raw`
WHERE `data`.amount IS NOT NULL;


-- ----------------------------------------------------------------------------
-- 2. Sliding-window moves: every 30 seconds, look back 5 minutes per symbol.
--    pct_change = (last price - first price) / first price * 100
--    FIRST_VALUE/LAST_VALUE aren't supported in HOP windows, so the first/last
--    price is taken via MIN/MAX over 'timestamp|price' strings.
-- ----------------------------------------------------------------------------
CREATE TABLE price_moves
WITH (
  'value.format' = 'json-registry'
) AS
SELECT
  symbol,
  window_start,
  window_end,
  open_price,
  close_price,
  low_price,
  high_price,
  (close_price - open_price) / open_price * 100 AS pct_change
FROM (
  SELECT
    symbol,
    window_start,
    window_end,
    CAST(SPLIT_INDEX(MIN(CONCAT(CAST(ts AS STRING), '|', CAST(price AS STRING))), '|', 1) AS DOUBLE) AS open_price,
    CAST(SPLIT_INDEX(MAX(CONCAT(CAST(ts AS STRING), '|', CAST(price AS STRING))), '|', 1) AS DOUBLE) AS close_price,
    MIN(price) AS low_price,
    MAX(price) AS high_price
  FROM TABLE(
    HOP(TABLE price_ticks, DESCRIPTOR($rowtime), INTERVAL '30' SECOND, INTERVAL '5' MINUTE)
  )
  GROUP BY symbol, window_start, window_end
);


-- ----------------------------------------------------------------------------
-- 3. Alerts: interval-join every window result against user rules.
--    A rule only matches moves that happen AFTER it was created (r.$rowtime),
--    so creating a rule never replays history. Direction/threshold are the
--    user's own settings, streamed in from the app via the alert_rules topic.
-- ----------------------------------------------------------------------------
CREATE TABLE alerts
WITH (
  'value.format' = 'json-registry'
) AS
SELECT
  r.rule_id,
  r.user_id,
  m.symbol,
  r.direction,
  r.threshold_pct,
  m.pct_change,
  m.open_price,
  m.close_price,
  m.`$rowtime` AS fired_at  -- not window_end: HOP bounds are zone-less (session time zone)
FROM price_moves AS m
JOIN alert_rules AS r
  ON m.symbol = r.symbol
 AND m.`$rowtime` BETWEEN r.`$rowtime` AND r.`$rowtime` + INTERVAL '30' DAY
WHERE (r.direction = 'DROP' AND m.pct_change <= -r.threshold_pct)
   OR (r.direction = 'RISE' AND m.pct_change >=  r.threshold_pct);


-- ----------------------------------------------------------------------------
-- Handy checks while demoing
-- ----------------------------------------------------------------------------
-- SELECT * FROM price_moves;
-- SELECT * FROM alert_rules;
-- SELECT * FROM alerts;
