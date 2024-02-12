CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q1 AS
SELECT
(data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint AS auction,
(data -> 'actualEvent' -> 'actualEvent' ->>'bidder')::bigint AS bidder,
(data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint * 0.908 AS price,
(data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp AS entryTime,
(data -> 'actualEvent' -> 'actualEvent' ->>'extra') ::text AS extra
FROM bids_source;
