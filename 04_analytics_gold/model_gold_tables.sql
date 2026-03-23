CREATE OR REPLACE TEMP VIEW gold_xlm_hourly AS
SELECT
  date_trunc('hour', event_ts) AS hour_ts,
  AVG(price_usd) AS avg_price_usd,
  SUM(volume_xlm) AS total_volume_xlm,
  SUM(notional_usd) AS total_notional_usd,
  COUNT(1) AS tx_count
FROM xlm_silver
GROUP BY date_trunc('hour', event_ts);

CREATE OR REPLACE TEMP VIEW gold_xlm_daily AS
SELECT
  date_trunc('day', event_ts) AS day_ts,
  AVG(price_usd) AS avg_price_usd,
  SUM(volume_xlm) AS total_volume_xlm,
  SUM(notional_usd) AS total_notional_usd,
  COUNT(1) AS tx_count
FROM xlm_silver
GROUP BY date_trunc('day', event_ts);

CREATE OR REPLACE TEMP VIEW gold_xlm_by_country_daily AS
SELECT
  date_trunc('day', s.event_ts) AS day_ts,
  u.country_code,
  AVG(s.price_usd) AS avg_price_usd,
  SUM(s.volume_xlm) AS total_volume_xlm,
  SUM(s.notional_usd) AS total_notional_usd,
  COUNT(1) AS tx_count
FROM xlm_silver s
JOIN users u
  ON s.user_id = u.user_id
GROUP BY date_trunc('day', s.event_ts), u.country_code;
