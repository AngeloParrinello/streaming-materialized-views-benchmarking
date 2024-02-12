SET 'auto.offset.reset' = 'earliest';

CREATE
    TYPE IF NOT EXISTS BID
    AS STRUCT <
                auction BIGINT,
                bidder BIGINT,
                price BIGINT,
                channel VARCHAR,
                url VARCHAR,
                entryTime VARCHAR,
               extra VARCHAR
             >;

CREATE
    TYPE IF NOT EXISTS ACTUAL_BID_EVENT
    AS STRUCT <
                    actualEvent BID,
                    eventType STRING
              >;

CREATE
OR REPLACE
STREAM IF NOT EXISTS total_bids (
                        wallclockTimestamp BIGINT,
                        eventTimestamp BIGINT,
                        actualEvent ACTUAL_BID_EVENT,
                        watermark BIGINT
                        ) WITH (
                        KAFKA_TOPIC='bids',
                        VALUE_FORMAT='JSON',
                        PARTITIONS=1,
                        REPLICAS=1,
                        -- only on remote cluster PARTITIONS=24,
                        -- only on remote cluster REPLICAS=3,
                        TIMESTAMP='eventTimestamp'
                        );

CREATE
OR REPLACE
STREAM bids WITH (
    KAFKA_TOPIC='minimal_bids',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
    -- only on remote cluster PARTITIONS=24,
    -- only on remote cluster REPLICAS=3
)
AS SELECT
    actualEvent -> actualEvent -> auction,
    actualEvent -> actualEvent -> bidder,
    actualEvent -> actualEvent -> price,
    actualEvent -> actualEvent -> channel,
    actualEvent -> actualEvent -> url,
    UNIX_TIMESTAMP(actualEvent -> actualEvent -> entryTime) as entryTime,
    actualEvent -> actualEvent -> extra
FROM total_bids
EMIT CHANGES;

-- For the real test I first created a stream that belongs to the bids topic (the real one, with all the information).
-- Then I created a stream that belongs to the minimal_bids topic (the one with only the information needed for the query).
-- The stream above, the one linked to the minimal_bids topic, was only used to fill the topic with the cleaned data.
-- Then, once the topic was full, I dropped the previous stream (the above one, linked to the just created minimal_bids topic) and I created a new one
-- a smaller and more efficient stream, that does not consume a lot of resource because it's totally rely on the minimal_bids topic.
-- This is the stream below.
/*CREATE
OR REPLACE
STREAM IF NOT EXISTS bids (
        auction BIGINT,
                bidder BIGINT,
                price BIGINT,
                channel VARCHAR,
                url VARCHAR,
                entryTime BIGINT,
               extra VARCHAR
                        ) WITH (
                        KAFKA_TOPIC='minimal_bids',
                        VALUE_FORMAT='JSON',
                        TIMESTAMP='entryTime',
                        PARTITIONS=24,
                        REPLICAS=3
                        )*/

CREATE
    TYPE IF NOT EXISTS AUCTION
    AS STRUCT <
                id BIGINT,
                itemName VARCHAR,
                description VARCHAR,
                initialBid BIGINT,
                reserve BIGINT,
                entryTime VARCHAR,
                expirationTime VARCHAR,
                seller BIGINT,
                category BIGINT,
                extra VARCHAR
             >;

CREATE
    TYPE IF NOT EXISTS ACTUAL_AUCTION_EVENT
    AS STRUCT <
                    actualEvent AUCTION,
                    eventType STRING
              >;

CREATE
STREAM IF NOT EXISTS total_auctions (
                        wallclockTimestamp BIGINT,
                        eventTimestamp BIGINT,
                        actualEvent ACTUAL_AUCTION_EVENT,
                        watermark BIGINT
                        ) WITH (
                        KAFKA_TOPIC='auctions',
                        VALUE_FORMAT='JSON',
                        PARTITIONS=1,
                        REPLICAS=1,
                        -- only on remote cluster PARTITIONS=24,
                        -- only on remote cluster REPLICAS=3,
                        TIMESTAMP='eventTimestamp'
                        );

CREATE
OR REPLACE
STREAM auctions WITH (
    KAFKA_TOPIC='minimal_auctions',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
    -- only on remote cluster PARTITIONS=24,
    -- only on remote cluster REPLICAS=3
    )
AS SELECT
    actualEvent -> actualEvent -> id,
    actualEvent -> actualEvent -> itemName,
    actualEvent -> actualEvent -> description,
    actualEvent -> actualEvent -> initialBid,
    actualEvent -> actualEvent -> reserve,
    UNIX_TIMESTAMP(actualEvent -> actualEvent -> entryTime) as entryTime,
    UNIX_TIMESTAMP(actualEvent -> actualEvent -> expirationTime) as expirationTime,
    actualEvent -> actualEvent -> seller,
    actualEvent -> actualEvent -> category,
    actualEvent -> actualEvent -> extra
FROM total_auctions
EMIT CHANGES;

-- For the real test I first created a stream that belongs to the auctions topic (the real one, with all the information).
-- Then I created a stream that belongs to the minimal_auctions topic (the one with only the information needed for the query).
-- The stream above, the one linked to the minimal_auctions topic, was only used to fill the topic with the cleaned data.
-- Then, once the topic was full, I dropped the previous stream (the above one, linked to the just created minimal_auctions topic) and I created a new one
-- a smaller and more efficient stream, that does not consume a lot of resource because it's totally rely on the minimal_auctions topic.
-- This is the stream below.

/*CREATE
OR REPLACE
STREAM IF NOT EXISTS auctions (
        id BIGINT,
                itemName VARCHAR,
                description VARCHAR,
                initialBid BIGINT,
                reserve BIGINT,
                entryTime BIGINT,
                expirationTime BIGINT,
                seller BIGINT,
                category BIGINT,
                extra VARCHAR
) WITH (
                        KAFKA_TOPIC='minimal_auctions',
                        VALUE_FORMAT='JSON',
                        TIMESTAMP='entryTime',
                        PARTITIONS=24,
                        REPLICAS=3
                        )*/

CREATE
    TYPE IF NOT EXISTS PERSON
    AS STRUCT <
                id BIGINT,
                name VARCHAR,
                email VARCHAR,
                creditCard VARCHAR,
                city VARCHAR,
                state VARCHAR,
                entryTime VARCHAR,
                extra VARCHAR
             >;

CREATE
    TYPE IF NOT EXISTS ACTUAL_PERSON_EVENT
    AS STRUCT <
                    actualEvent PERSON,
                    eventType STRING
              >;

CREATE
STREAM IF NOT EXISTS total_people (
                        wallclockTimestamp BIGINT,
                        eventTimestamp BIGINT,
                        actualEvent ACTUAL_PERSON_EVENT,
                        watermark BIGINT
                        ) WITH (
                        KAFKA_TOPIC='people',
                        VALUE_FORMAT='JSON',
                        PARTITIONS=1,
                        REPLICAS=1,
                        -- only on remote cluster PARTITIONS=24,
                        -- only on remote cluster REPLICAS=3,
                        TIMESTAMP='eventTimestamp'
                        );

CREATE
OR REPLACE
STREAM people WITH (
    KAFKA_TOPIC='minimal_people',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
    -- only on remote cluster PARTITIONS=24,
    -- only on remote cluster REPLICAS=3
    )
AS SELECT
    actualEvent -> actualEvent -> id,
    actualEvent -> actualEvent -> name,
    actualEvent -> actualEvent -> email,
    actualEvent -> actualEvent -> creditCard,
    actualEvent -> actualEvent -> city,
    actualEvent -> actualEvent -> state,
    UNIX_TIMESTAMP(actualEvent -> actualEvent -> entryTime) as entryTime,
    actualEvent -> actualEvent -> extra
FROM total_people
EMIT CHANGES;

-- For the real test I first created a stream that belongs to the people topic (the real one, with all the information).
-- Then I created a stream that belongs to the minimal_people topic (the one with only the information needed for the query).
-- The stream above, the one linked to the minimal_people topic, was only used to fill the topic with the cleaned data.
-- Then, once the topic was full, I dropped the previous stream (the above one, linked to the just created minimal_people topic) and I created a new one
-- a smaller and more efficient stream, that does not consume a lot of resource because it's totally rely on the minimal_people topic.
-- This is the stream below.

/*CREATE
OR REPLACE
STREAM IF NOT EXISTS people (
        id BIGINT,
        name VARCHAR,
        email VARCHAR,
        creditCard VARCHAR,
        city VARCHAR,
        state VARCHAR,
        entryTime BIGINT,
        extra VARCHAR
) WITH (
                        KAFKA_TOPIC='minimal_people',
                        VALUE_FORMAT='JSON',
                        TIMESTAMP='entryTime',
                        PARTITIONS=24,
                        REPLICAS=3
                        )*/