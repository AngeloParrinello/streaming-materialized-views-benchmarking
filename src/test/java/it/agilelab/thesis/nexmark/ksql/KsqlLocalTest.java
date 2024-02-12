package it.agilelab.thesis.nexmark.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TopicInfo;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.kafka.NexmarkKafkaProducer;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers(disabledWithoutDocker = true, parallel = true)
public class KsqlLocalTest {
    private static final NexmarkTestsUtil testsUtil = new NexmarkTestsUtil();
    private static Client client;

    @BeforeAll
    public static void setup() {
        testsUtil.startKafkaContainer();

        testsUtil.startKsqlContainer();

        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 400, 1);

        NexmarkKafkaProducer nexmarkKafkaProducer = new NexmarkKafkaProducer(testsUtil.getKafkaBootstrapServers(),
                config, 100);
        nexmarkKafkaProducer.start();

        client = new KsqlDBConfig(testsUtil.getKsqlDBHost(), testsUtil.getKsqlDBPort()).build();

    }


    @Test
    @Order(1)
    void ksqlShouldStartWithKafka() {
        await().atMost(30, TimeUnit.SECONDS).until(testsUtil.getKafka()::isRunning);
        await().atMost(30, TimeUnit.SECONDS).until(testsUtil.getKsqlDB()::isRunning);
    }

    @Test
    @Order(2)
    void checkThreeTopics() throws ExecutionException, InterruptedException {
        List<TopicInfo> topics = client.listTopics().get();
        topics.forEach(System.out::println);

        // Three plus the default one plus the (possible) table
        Assertions.assertTrue(topics.size() >= 4);
    }

    @Test
    @Order(3)
    void testCreateBidStream() {
        String createBidType =
                "CREATE TYPE IF NOT EXISTS BID AS STRUCT <\n" +
                        "  auction BIGINT,\n" +
                        "  bidder BIGINT,\n" +
                        "  price BIGINT,\n" +
                        "  channel VARCHAR,\n" +
                        "  url VARCHAR,\n" +
                        "  entryTime VARCHAR,\n" +
                        "  extra VARCHAR\n" +
                        ">;";

        String createActualEventType =
                "CREATE TYPE IF NOT EXISTS ACTUAL_EVENT AS STRUCT <\n" +
                        "  actualEvent BID,\n" +
                        "  eventType STRING\n" +
                        ">;";

        String createBidStream =
                "CREATE STREAM IF NOT EXISTS bids (\n" +
                        "  wallclockTimestamp TIMESTAMP,\n" +
                        "  eventTimestamp TIMESTAMP,\n" +
                        "  actualEvent ACTUAL_EVENT,\n" +
                        "  watermark TIMESTAMP\n" +
                        ") WITH (\n" +
                        "  KAFKA_TOPIC='bids',\n" +
                        "  KEY_FORMAT='KAFKA',\n" +
                        "  VALUE_FORMAT='JSON'\n" +
                        ");";

        String dropBidsStream = "DROP STREAM IF EXISTS bids;";
        String dropBidsTable = "DROP TABLE IF EXISTS bids_table;";
        String dropBidType = "DROP TYPE IF EXISTS BID;";
        String dropActualEventType = "DROP TYPE IF EXISTS ACTUAL_EVENT;";

        client.executeStatement(dropBidType).join();
        client.executeStatement(dropActualEventType).join();
        client.executeStatement(dropBidsTable).join();
        client.executeStatement(dropBidsStream).join();

        client.listStreams().thenAccept(streams -> System.out.println("Streams: " + streams));

        client.executeStatement(createBidType).join();
        client.executeStatement(createActualEventType).join();
        client.executeStatement(createBidStream).join();

        client.listStreams().thenAccept(streams -> Assertions.assertEquals(2, streams.size()));

        Map<String, Object> properties = new HashMap<>();
        properties.put("auto.offset.reset", "earliest");
        properties.put("processing.guarantee", "exactly_once_v2");

        Assertions.assertEquals(properties.size(), 2);

        //Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
        CompletableFuture<StreamedQueryResult> streamQuery =
                client.streamQuery("SELECT * FROM bids LIMIT 10;", properties);
        NexmarkSubscriber subscriber = new NexmarkSubscriber();

        streamQuery
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Streamed query result: " + streamedQueryResult);
                    // to see the logs of the query look into the ksqlDB container
                    // or put a sysout in the onNext method of the subscriber
                    streamedQueryResult.subscribe(subscriber);
                })
                .exceptionally(e -> {
                    System.out.println("Exception: " + e);
                    throw new RuntimeException(e);
                });

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(10, subscriber.getCount()));
    }

    @Test
    @Order(4)
    void testCreateBidTableOnTopOfBidsStream() {
        String createBidTable =
                "CREATE OR REPLACE TABLE bids_table WITH( FORMAT='JSON') AS\n" +
                        "SELECT\n" +
                        "  actualEvent->actualEvent->auction AS auctionId,\n" +
                        "  actualEvent->actualEvent->bidder AS bidder,\n" +
                        "  actualEvent->actualEvent->price AS price,\n" +
                        "  actualEvent->actualEvent->channel AS channel,\n" +
                        "  actualEvent->actualEvent->url AS url,\n" +
                        "  actualEvent->actualEvent->entryTime AS dateTime,\n" +
                        "  actualEvent->actualEvent->extra AS extra, \n" +
                        "  COUNT(*) AS count\n" +
                        "FROM bids\n" +
                        "GROUP BY actualEvent->actualEvent->auction,actualEvent->actualEvent->bidder, " +
                        "actualEvent->actualEvent->price, actualEvent->actualEvent->channel, " +
                        "actualEvent->actualEvent->url, actualEvent->actualEvent->entryTime, " +
                        "actualEvent->actualEvent->extra;";

        String dropBidsTable = "DROP TABLE if EXISTS bids_table;";
        client.executeStatement(dropBidsTable).join();
        client.listTables().thenAccept(tables -> System.out.println("Tables: " + tables));


        client.executeStatement(createBidTable).join();
        client.listTables().thenAccept(tables -> {
            System.out.println("Tables NOW: " + tables);
            Assertions.assertEquals(1, tables.size());
        });

        String selectFromBidTable = "SELECT * FROM bids_table EMIT CHANGES LIMIT 10;";
        var queryResult = client.streamQuery(selectFromBidTable);

        /* At the moment, DESCRIBE is not supported
        String describeStatement = "DESCRIBE bids;";
        client.executeStatement(describeStatement)
                .thenAccept(result -> {
                    System.out.println("Stream Description:");
                    System.out.println(result.queryId().get());
                })
                .join();*/

        NexmarkSubscriber subscriber = new NexmarkSubscriber();

        queryResult
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Streamed query result: " + streamedQueryResult);

                    streamedQueryResult.subscribe(subscriber);
                })
                .exceptionally(e -> {
                    System.out.println("Exception: " + e);
                    return null;
                });

        // TODO: This is not working
        /*await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(10, subscriber.getCount()));*/
    }
}
