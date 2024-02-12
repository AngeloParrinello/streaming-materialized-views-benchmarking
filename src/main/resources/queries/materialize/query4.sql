CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q4 AS
SELECT Q.category as category,
       AVG(Q.final) as avg
FROM (SELECT MAX((B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint) AS final,
    (A.data -> 'actualEvent' -> 'actualEvent' ->>'category')::bigint as category
      FROM auctions_source AS A,
           bids_source AS B
      WHERE (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint
        = (B.data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint
        AND ( (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp
            BETWEEN (A.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime')::timestamp
                AND (A.data -> 'actualEvent' -> 'actualEvent' ->>'expirationTime')::timestamp )
      GROUP BY (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint,
        (A.data -> 'actualEvent' -> 'actualEvent' ->>'category')::bigint) AS Q
GROUP BY Q.category;