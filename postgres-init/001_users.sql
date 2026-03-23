CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  country_code TEXT NOT NULL
);

INSERT INTO users (user_id, country_code) VALUES
  (1, 'BR'),
  (2, 'US'),
  (3, 'DE'),
  (4, 'GB'),
  (5, 'IN'),
  (6, 'JP'),
  (7, 'CA'),
  (8, 'FR'),
  (9, 'ES'),
  (10, 'MX')
ON CONFLICT (user_id) DO UPDATE
SET country_code = EXCLUDED.country_code;
