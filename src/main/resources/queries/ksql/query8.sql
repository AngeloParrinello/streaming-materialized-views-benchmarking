-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE OR REPLACE TABLE subnexmark1_q8 WITH(KAFKA_TOPIC='SUB_RESULT1_QUERY8', FORMAT='JSON') AS
SELECT AS_VALUE(id) as id,
       AS_VALUE(name) as name,
       TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS window_start,
       TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss')   AS window_end,
       id as idKey,
       name as nameKey,
       COUNT(*) AS num
       FROM people
       -- creates a tumbling window of 10 seconds that holds the people that have entered the system in the last 10 seconds
       WINDOW TUMBLING (SIZE 10 SECONDS)
       GROUP BY id, name
       EMIT CHANGES;

CREATE
OR REPLACE STREAM IF NOT EXISTS subnexmark1_q8_stream (window_start VARCHAR, window_end VARCHAR, id BIGINT, name VARCHAR) WITH (KAFKA_TOPIC='SUB_RESULT1_QUERY8', VALUE_FORMAT='JSON');

CREATE OR REPLACE TABLE subnexmark2_q8 WITH(KAFKA_TOPIC='SUB_RESULT2_QUERY8', FORMAT='JSON') AS
    SELECT AS_VALUE(seller) as seller,
           TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS window_start,
           TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss')   AS window_end,
           seller as sellerKey,
           COUNT(*) AS num
    FROM auctions
    -- creates a tumbling window of 10 seconds that holds the auctions that have entered the system in the last 10 seconds
    WINDOW TUMBLING (SIZE 10 SECONDS)
    -- remember that the WINDOWSTART and WINDOWEND are already in the group by and in the key
    GROUP BY seller
    EMIT CHANGES;

CREATE
OR REPLACE STREAM IF NOT EXISTS subnexmark2_q8_stream (window_start VARCHAR, window_end VARCHAR, seller BIGINT) WITH (KAFKA_TOPIC='SUB_RESULT2_QUERY8', VALUE_FORMAT='JSON');

CREATE OR REPLACE TABLE nexmark_q8 WITH(KAFKA_TOPIC='RESULT_QUERY8', FORMAT='JSON') AS
-- we need to wrap some values in AS_VALUE because in this way we have that field in the key and in the value of the kafka message (inside the topic)
-- in fact, if we don't do this, we have the field only in the key of the kafka message and not in the value. And if we
-- wanted to use it from the topic we would receive a null value. In this way we avoid this problem.
SELECT AS_VALUE(P.id) as id, AS_VALUE(P.name) as name, AS_VALUE(P.window_start) as window_start, P.window_start as window_start_key, P.id as idKey, P.name as nameKey, COUNT(*) AS num
FROM subnexmark1_q8_stream AS P
INNER JOIN subnexmark2_q8_stream AS A WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
ON P.id = A.seller
WHERE P.window_start = A.window_start AND P.window_end = A.window_end
GROUP BY P.id, P.name, P.window_start
EMIT CHANGES;