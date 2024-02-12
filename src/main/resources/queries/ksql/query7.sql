-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- We need to group by on the auction and we cannot do it on the window bounds due to
-- See https://github.com/confluentinc/ksql/issues/4397
-- This lead to a cardinality slightly higher than the other translated queries,
-- but the results are the same.
SET 'auto.offset.reset' = 'earliest';

CREATE
OR REPLACE TABLE subnexmark1_q7 WITH(KAFKA_TOPIC='SUB_RESULT1_QUERY7', FORMAT='JSON') AS
SELECT TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS window_start,
       TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss')   AS window_end,
       MAX(price)          AS highest_bid,
       auction
FROM bids
    WINDOW TUMBLING (SIZE 10 SECONDS)
GROUP BY auction
    EMIT CHANGES;

CREATE
OR REPLACE STREAM IF NOT EXISTS subnexmark1_q7_stream (window_start VARCHAR, window_end VARCHAR, highest_bid BIGINT) WITH (KAFKA_TOPIC='SUB_RESULT1_QUERY7', VALUE_FORMAT='JSON');

CREATE
OR REPLACE TABLE subnexmark2_q7 WITH(KAFKA_TOPIC='SUB_RESULT2_QUERY7', FORMAT='JSON') AS
SELECT window_start,
       window_end,
       TOPK(highest_bid, 1) AS highest_bid
FROM subnexmark1_q7_stream
GROUP BY window_start, window_end EMIT CHANGES;

CREATE
OR REPLACE STREAM IF NOT EXISTS subnexmark2_q7_stream (highest_bid ARRAY<BIGINT>) WITH (KAFKA_TOPIC='SUB_RESULT2_QUERY7', VALUE_FORMAT='JSON');

CREATE
OR REPLACE STREAM nexmark_q7 WITH(KAFKA_TOPIC='RESULT_QUERY7', FORMAT='JSON') AS
SELECT B.auction,
       B.price,
       B.entryTime,
       B.bidder,
       B.extra
FROM bids AS B
         INNER JOIN subnexmark2_q7_stream AS S WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
ON B.price = S.highest_bid[1]
EMIT CHANGES;

