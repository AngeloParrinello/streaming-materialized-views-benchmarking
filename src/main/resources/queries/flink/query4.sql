-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the winning bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q4
(
    id    BIGINT,
    final BIGINT
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q4
SELECT Q.category,
       AVG(Q.final)
FROM (SELECT MAX(B.price) AS final, A.category
      FROM auctions A,
           bids B
      WHERE A.id = B.auction
        AND B.entryTime BETWEEN A.entryTime AND A.expirationTime
      GROUP BY A.id, A.category) Q
GROUP BY Q.category;