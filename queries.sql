-- 1) Load CSVs (DuckDB)
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS assignments;
DROP TABLE IF EXISTS events;

CREATE TABLE users AS
SELECT * FROM read_csv_auto('data/users.csv', HEADER=TRUE);

CREATE TABLE assignments AS
SELECT * FROM read_csv_auto('data/assignments.csv', HEADER=TRUE);

CREATE TABLE events AS
SELECT * FROM read_csv_auto('data/events.csv', HEADER=TRUE);

-- 2) Exposure (who's in the experiment)
CREATE OR REPLACE VIEW exposure AS
SELECT a.user_id, a.variant
FROM assignments a;

-- 3) Clicks (CTR on day 0)
CREATE OR REPLACE VIEW clicks AS
SELECT e.user_id, 1 AS clicked
FROM events e
WHERE e.event = 'click' AND e.day_offset = 0
GROUP BY e.user_id;

-- 4) D1 retention proxy (login on day 1)
CREATE OR REPLACE VIEW d1 AS
SELECT e.user_id, 1 AS d1_retained
FROM events e
WHERE e.event = 'login' AND e.day_offset = 1
GROUP BY e.user_id;

-- 5) Master table per user
CREATE OR REPLACE VIEW user_metrics AS
SELECT
  ex.user_id,
  ex.variant,
  COALESCE(c.clicked, 0) AS clicked,
  COALESCE(d.d1_retained, 0) AS d1_retained
FROM exposure ex
LEFT JOIN clicks c  ON ex.user_id = c.user_id
LEFT JOIN d1 d      ON ex.user_id = d.user_id;

-- 6) CTR and D1 by variant
--    Also computes stderr, z, p for CTR difference (B vs A)
WITH agg AS (
  SELECT
    variant,
    COUNT(*)::DOUBLE AS n,
    SUM(clicked)::DOUBLE AS clicks,
    AVG(clicked)::DOUBLE AS ctr,
    AVG(d1_retained)::DOUBLE AS d1
  FROM user_metrics
  GROUP BY 1
),
pairs AS (
  SELECT
    a.ctr AS ctr_a, b.ctr AS ctr_b,
    a.n   AS n_a,   b.n   AS n_b,
    (b.ctr - a.ctr) AS abs_lift,
    (CASE WHEN a.ctr>0 THEN (b.ctr/a.ctr - 1) ELSE NULL END) AS rel_lift,
    a.d1  AS d1_a,  b.d1  AS d1_b
  FROM agg a
  JOIN agg b ON a.variant='A' AND b.variant='B'
),
ztest AS (
  SELECT
    *,
    /* standard error for difference in proportions */
    sqrt( (ctr_a*(1-ctr_a))/n_a + (ctr_b*(1-ctr_b))/n_b ) AS se,
    CASE
      WHEN ( (ctr_a*(1-ctr_a))/n_a + (ctr_b*(1-ctr_b))/n_b ) = 0
      THEN NULL
      ELSE (ctr_b - ctr_a) / sqrt( (ctr_a*(1-ctr_a))/n_a + (ctr_b*(1-ctr_b))/n_b )
    END AS z
  FROM pairs
)
SELECT
  ctr_a, ctr_b,
  abs_lift, rel_lift,
  d1_a, d1_b,
  se, z,
  /* two-sided p-value via normal CDF */
  2 * (1 - CAST(norm_cdf(ABS(z)) AS DOUBLE)) AS p_value
FROM ztest;

-- 7) Daily CTR by variant (trend view)
SELECT
  e.day_offset,
  ex.variant,
  AVG(CASE WHEN e.event='click' AND e.day_offset=0 THEN 1 ELSE 0 END)::DOUBLE AS ctr_d0
FROM exposure ex
LEFT JOIN events e ON ex.user_id = e.user_id
GROUP BY 1,2
ORDER BY 1,2;

-- 8) Sanity checks
-- Balance check
SELECT variant, COUNT(*) AS users FROM exposure GROUP BY 1;

-- Click-through counts
SELECT variant, SUM(clicked) AS clicks, COUNT(*) AS users, AVG(clicked)::DOUBLE AS ctr
FROM user_metrics
GROUP BY 1;
