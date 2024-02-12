package it.agilelab.thesis.nexmark.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkTestsUtil;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.kafka.NexmarkKafkaProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NexmarkQueriesKsqlTest {

    private static final NexmarkTestsUtil testsUtil = new NexmarkTestsUtil();
    private static Client client;

    @BeforeAll
    public static void setup() throws IOException {
        testsUtil.startKafkaContainer();

        testsUtil.startKsqlContainer();

        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 500, 1);

        NexmarkKafkaProducer nexmarkKafkaProducer = new NexmarkKafkaProducer(testsUtil.getKafkaBootstrapServers(),
                config, 100);
        nexmarkKafkaProducer.start();

        client = new KsqlDBConfig(testsUtil.getKsqlDBHost(), testsUtil.getKsqlDBPort()).build();

        createTypes();
    }

    @Test
    void testQuery1() {
        executeQuery("src/main/resources/queries/ksql/query1.sql");
    }

    @Test
    void testQuery2() {
        executeQuery("src/main/resources/queries/ksql/query2.sql");
    }

    @Test
    void testQuery3() {
        executeQuery("src/main/resources/queries/ksql/query3.sql");
    }

    @Test
    void testQuery4() {
        executeQuery("src/main/resources/queries/ksql/query4.sql");
    }

    @Disabled
    @Test
    void testQuery5() {
        executeQuery("src/main/resources/queries/ksql/query5.sql");
    }

    @Test
    void testQuery6() {
        executeQuery("src/main/resources/queries/ksql/query6.sql");
    }

    @Test
    void testQuery7() {
        executeQuery("src/main/resources/queries/ksql/query7.sql");
    }

    @Test
    void testQuery8() {
        executeQuery("src/main/resources/queries/ksql/query8.sql");
    }

    private static void createTypes() throws IOException {
        List<String> queries = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/ksql/create_types.sql");

        // remove the first line, which is a SET line, already defined in the properties
        queries.remove(0);

        queries.forEach(query -> client.executeStatement(query).join());
    }

    private void executeQuery(String queryPath) {
        try {
            List<String> queries = NexmarkUtil.readQueryFileAsString(queryPath);
            Map<String, Object> properties = new HashMap<>();
            properties.put("auto.offset.reset", "earliest");
            // properties.put("processing.guarantee", "exactly_once_v2");

            // remove the first line, which is a SET line, already defined in the properties
            queries.remove(0);

            for (String query : queries) {
                ExecuteStatementResult result = client.executeStatement(query, properties).join();
                result.queryId().ifPresent(id -> Assertions.assertTrue(id.contains("NEXMARK")));
            }
            // String selectFromBidTable = "SELECT * FROM nexmark_??? EMIT CHANGES;";

            // TODO: this is not working, does not print anything even if the query is running and the data are available
        /*
        client.streamQuery(selectFromBidTable, properties)
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());

                    NexmarkSubscriber subscriber = new NexmarkSubscriber();
                    streamedQueryResult.subscribe(subscriber);
                }).exceptionally(e -> {
                    System.out.println("Request failed: " + e);
                    return null;
                });*/

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
