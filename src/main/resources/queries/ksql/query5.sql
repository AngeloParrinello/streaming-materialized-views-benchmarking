-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------
-- Subquery 1 (AuctionBids): It calculates the number of bids for each auction within a specific time window.
-- This subquery uses the HOP function to create time-based windows of 10 seconds sliding every 2 seconds.
-- It counts the number of bids in each window for each auction.
-- -------------------------------------------------------------------------------------------------
-- Subquery 2 (MaxBids): This subquery calculates the maximum number of bids within the same time windows as
-- in Subquery 1. It groups the results by the same time window and calculates the maximum bid count within that window.
-- -------------------------------------------------------------------------------------------------
-- Main Query: The main query joins the results of Subquery 1 and Subquery 2 based on the time window and the bid count.
-- It selects auction IDs and the number of bids from the AuctionBids subquery where the bid count is greater
-- than or equal to the maximum bid count calculated in the MaxBids subquery.

-- What I have done until here:
-- 1) Tried to re-create the same query. Problems: ksqlDB does not support persistent query on windowed table, so I need to create a stream on the created topic instead of a persistent query
-- But this stream has some issue with windowed table (indeed, the auction field is not recognized as a field of the table and is null ... if I look inside the topic,
-- the auction field is not present in the value but only in the key and it is mixed with the window start time and end time)
-- 2) Since the auction field was not required in the next query, I created the next table (based on the stream and not on the first table)
-- and it seems to work. But now there is a problem here. ksqlDB does not support join on multiple conditions (see https://github.com/confluentinc/ksql/issues/8574)
-- 3) So I tried to put the second condition of the join in the where clause but it does not work because the join in ksql
-- must be done on the whole key. But again ksqldb require that during a stream-table join, the condition must be on the table's primary key which is,
-- as I said before the tuple (window_start, window_end) (see the error: Invalid join condition: stream-table joins require to join on the table's primary key).
-- Possible solutions: convert also the second table into a stream and made a stream-stream join or
-- create a struct (during the select statement) that encapsulate the two fields and use it as a key.

-- Second attempt

-- First: we create a table with only auction as key

-- SET 'auto.offset.reset' = 'earliest';

/*CREATE
OR REPLACE TABLE first_subquery1_nexmark_q5 WITH(KAFKA_TOPIC='FIRST_SUBQUERY1_RESULT_QUERY5', FORMAT='JSON') AS
SELECT  B.actualEvent -> actualEvent -> auction   AS auction,
        COUNT(*)                                AS num,
        WINDOWSTART                               AS startTime,
        WINDOWEND                                 AS endTime
FROM bids B
    WINDOW HOPPING (SIZE 10 SECOND, ADVANCE BY 2 SECOND)
    GROUP BY B.actualEvent -> actualEvent -> auction
    EMIT CHANGES;


-- Second: on the previous table we create another table to groupby on startTime and endTime

-- But ksqlDB does not support persistent query on windowed table so we need to create before a stream on the created topic

CREATE
STREAM first_subquery1_nexmark_q5_stream
       (num BIGINT, startTime BIGINT, endTime BIGINT, auction BIGINT)
         WITH (KAFKA_TOPIC='FIRST_SUBQUERY1_RESULT_QUERY5', VALUE_FORMAT='JSON');

CREATE
OR REPLACE TABLE second_subquery1_nexmark_q5 WITH(KAFKA_TOPIC='SECOND_SUBQUERY1_RESULT_QUERY5', FORMAT='JSON') AS
SELECT COUNT(*)                                AS num,
       CountBids.startTime                               AS startTime,
       CountBids.endTime                                 AS endTime,
       CountBids.auction                    AS auction
FROM first_subquery1_nexmark_q5_stream AS CountBids
GROUP BY CountBids.auction, CountBids.startTime, CountBids.endTime
EMIT CHANGES;*/


-- First attempt


/*CREATE
OR REPLACE TABLE subquery1_nexmark_q5 WITH(KAFKA_TOPIC='SUBQUERY1_RESULT_QUERY5', FORMAT='JSON') AS
                -- Subquery 1: Count the number of bids for each auction within a time window
SELECT count(*)                                  AS num,
       WINDOWSTART                               AS startTime,
       WINDOWEND                                 AS endTime,
       B.actualEvent -> actualEvent -> auction   AS auction
FROM bids B
    WINDOW HOPPING (SIZE 10 SECOND, ADVANCE BY 2 SECOND)
GROUP BY B.actualEvent -> actualEvent -> auction
    EMIT CHANGES;

-- Subquery 2: Find the maximum number of bids within a time window
-- ksqldb does not support persistent query on windowed table https://github.com/confluentinc/ksql/issues/6513 see also
-- https://github.com/confluentinc/ksql/issues/3984 for the context and why is not possible here https://github.com/confluentinc/ksql/issues/5519
-- So we need to a table on the created topic instead of a persistent query

CREATE
STREAM subquery1_stream
       (num BIGINT, startTime BIGINT, endTime BIGINT, auction BIGINT)
         WITH (KAFKA_TOPIC='SUBQUERY1_RESULT_QUERY5', VALUE_FORMAT='JSON');

CREATE
OR REPLACE TABLE subquery2_nexmark_q5 WITH(KAFKA_TOPIC='SUBQUERY2_RESULT_QUERY5', FORMAT='JSON') AS
SELECT MAX(CountBids.num)  AS maxn,
       CountBids.startTime AS startTime,
       CountBids.endTime   AS endTime
FROM subquery1_stream AS CountBids
-- joins on multiple conditions are not yet supported...
GROUP BY CountBids.startTime, CountBids.endTime EMIT CHANGES;

CREATE
OR REPLACE TABLE nexmark_q5 WITH(KAFKA_TOPIC='RESULT_QUERY5', FORMAT='JSON') AS
SELECT AuctionBids.auction, AuctionBids.num
FROM subquery1_stream AS AuctionBids
    INNER JOIN subquery2_nexmark_q5 AS MaxBids WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
-- Join the results of the two subqueries based on time window and bid count
ON AuctionBids.startTime = MaxBids.startTime
WHERE AuctionBids.endTime = MaxBids.endTime AND AuctionBids.num >= MaxBids.maxn
EMIT CHANGES;*/