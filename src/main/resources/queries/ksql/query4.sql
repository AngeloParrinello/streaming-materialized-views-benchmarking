-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the winning bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------

SET 'auto.offset.reset' = 'earliest';

CREATE OR REPLACE TABLE subnexmark_q4 WITH(KAFKA_TOPIC='SUB_RESULT_QUERY4', FORMAT='JSON') AS
    SELECT MAX(B.price) AS final, A.category, A.id
    FROM auctions A INNER JOIN bids B WITHIN 1 HOURS GRACE PERIOD 15 MINUTES
    ON A.id = B.auction
    WHERE (B.entryTime BETWEEN A.entryTime AND A.expirationTime)
    GROUP BY A.category, A.id
    EMIT CHANGES;

CREATE
OR REPLACE TABLE nexmark_q4 WITH(KAFKA_TOPIC='RESULT_QUERY4', FORMAT='JSON') AS
SELECT Q.category as category,
       AVG(Q.final) as avg
FROM subnexmark_q4 as Q
GROUP BY Q.category
EMIT CHANGES;