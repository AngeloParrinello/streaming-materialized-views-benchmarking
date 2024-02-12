CREATE
MATERIALIZED VIEW IF NOT EXISTS nexmark_q3 AS
SELECT (P.data -> 'actualEvent' -> 'actualEvent' ->>'name') as name,
       (P.data -> 'actualEvent' -> 'actualEvent' ->>'city') as city,
       (P.data -> 'actualEvent' -> 'actualEvent' ->>'state') as state,
       (A.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint as id,
       (A.data -> 'actualEvent' -> 'actualEvent' ->>'category')::bigint as category
FROM auctions_source AS A
         INNER JOIN people_source AS P
             on (A.data -> 'actualEvent' -> 'actualEvent' ->>'seller')::bigint =
              (P.data -> 'actualEvent' -> 'actualEvent' ->>'id')::bigint
WHERE (A.data -> 'actualEvent' -> 'actualEvent' ->>'category')::bigint = 10
  AND ( (P.data -> 'actualEvent' -> 'actualEvent' ->>'state') = 'OR'
    OR (P.data -> 'actualEvent' -> 'actualEvent' ->>'state') = 'ID'
    OR (P.data -> 'actualEvent' -> 'actualEvent' ->>'state') = 'CA');