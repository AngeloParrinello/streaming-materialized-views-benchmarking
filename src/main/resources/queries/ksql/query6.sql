-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same ‘winning bids’ core as for Query4.
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE OR REPLACE STREAM subnexmark1_q6 WITH(KAFKA_TOPIC='SUB_RESULT1_QUERY6', FORMAT='JSON') AS
SELECT A.id, A.seller,
       B.price, B.entryTime
FROM auctions AS A INNER JOIN bids AS B WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
ON A.id = B.auction
WHERE (B.entryTime between A.entryTime and A.expirationTime )
EMIT CHANGES;

CREATE OR REPLACE TABLE subnexmark2_q6 WITH(KAFKA_TOPIC='SUB_RESULT2_QUERY6', FORMAT='JSON') AS
SELECT TOPK(price, 10) AS top10, AS_VALUE(seller) as seller, seller as sellerKey
FROM subnexmark1_q6
GROUP BY seller
EMIT CHANGES;

CREATE
OR REPLACE STREAM IF NOT EXISTS subnexmark2_q6_stream (top10 ARRAY<BIGINT>, seller BIGINT) WITH (KAFKA_TOPIC='SUB_RESULT2_QUERY6', VALUE_FORMAT='JSON');

CREATE OR REPLACE STREAM subnexmark3_q6 WITH(KAFKA_TOPIC='SUB_RESULT3_QUERY6', FORMAT='JSON') AS
SELECT seller, EXPLODE(top10) AS top10_single
FROM subnexmark2_q6_stream
EMIT CHANGES;

CREATE OR REPLACE TABLE
IF NOT EXISTS nexmark_q6 AS
SELECT seller, AVG(top10_single) AS avg
FROM subnexmark3_q6
GROUP BY seller
EMIT CHANGES;

