CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q2 AS
SELECT
(data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint as auction,
(data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint as price
FROM bids_source
WHERE (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint = 1007
OR (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint = 1020
OR (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint = 2001
OR (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint = 2019
OR (data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint = 2087;