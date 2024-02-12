CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q6 AS
SELECT W.seller as seller,
       AVG(price) as avg
FROM
(SELECT DISTINCT (data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint as seller FROM auctions_source) AS W,
    LATERAL (
    SELECT
        (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint as id,
        (A.data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint as seller,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint as price,
        (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp as entryTime
    FROM
        auctions_source AS A,
        bids_source AS B
    WHERE
        (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint =
            (B.data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint
    AND
        ((B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp
            BETWEEN (A.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp
            AND (A.data -> 'actualEvent' -> 'actualEvent' ->>'expirationTime'):: timestamp)
    AND (A.data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint = W.seller
    ORDER BY (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint DESC LIMIT 10)
GROUP BY W.seller;

-- The query above is equivalent to the following SQL query. The nexmark paper, when describing the query 6,
-- says that the query uses a window function to get the top 10 bids per auction. Indeed, the query below uses the
-- "pure" window function. However, the query below is less efficient than the one above,
-- but it is equivalent and easier to understand.
-- See here: https://materialize.com/docs/transform-data/patterns/window-functions/ for more information
/*SELECT seller as seller,
       AVG(price) as avg
 FROM
    (
    SELECT seller, price, ROW_NUMBER() OVER (PARTITION BY seller ORDER BY price DESC) AS row_num
    FROM (
    SELECT (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint as id, (A.data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint as seller, (B.data -> 'actualEvent' -> 'actualEvent' ->>'price')::bigint as price, (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp as entryTime
    FROM auctions_source AS A, bids_source AS B
    WHERE (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint
    = (B.data -> 'actualEvent' -> 'actualEvent' ->>'auction')::bigint
    AND ( (B.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp
    BETWEEN (A.data -> 'actualEvent' -> 'actualEvent' ->>'entryTime'):: timestamp
    AND (A.data -> 'actualEvent' -> 'actualEvent' ->>'expirationTime'):: timestamp )
    )

    )
WHERE row_num <= 10
GROUP BY seller LIMIT 10*/

