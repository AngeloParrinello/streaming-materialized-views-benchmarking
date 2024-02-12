package it.agilelab.thesis.nexmark.materialize;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.kafka.NexmarkKafkaProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIf;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledIf(value = "it.agilelab.thesis.nexmark.NexmarkTestsUtil#isKafkaContainerNotRunning", disabledReason = "Local test")
@Testcontainers(disabledWithoutDocker = true, parallel = true)
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
public class MaterializeConfigTest {
    private static Connection connection;

    @BeforeAll
    public static void setup() throws SQLException {
        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 400, 1);

        NexmarkKafkaProducer nexmarkProducer = new NexmarkKafkaProducer("localhost:29092",
                config, 100);
        nexmarkProducer.start();

        connection = MaterializeConfig.connect();
    }

    @Test
    @Order(1)
    public void testConnect() {
        try {
            Assertions.assertFalse(connection.isClosed());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    @Order(2)
    public void testCreateConnection() {
        try {
            String createConnection =
                    " CREATE CONNECTION IF NOT EXISTS kafka_connection" +
                            // should connect to broker:9092 the right advertised listener set in the docker-compose file
                            // remember that we're in a docker network
                            " TO KAFKA (BROKER 'broker:9092');";
            Statement createConnectionStatement = connection.createStatement();
            var result = createConnectionStatement.execute(createConnection);
            System.out.println("Connection created.");
            createConnectionStatement.close();
            Assertions.assertFalse(result);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    @Test
    @Order(3)
    public void testCreateSource() {
        try {
            String createSource =
                    " CREATE SOURCE IF NOT EXISTS bids_source " +
                            " FROM KAFKA CONNECTION kafka_connection (TOPIC 'bids') " +
                            " KEY FORMAT TEXT" +
                            " VALUE FORMAT JSON " +
                            " WITH (SIZE = '1');  ";
            Statement createSourceStatement = connection.createStatement();
            var result = createSourceStatement.execute(createSource);
            System.out.println("Source created.");
            createSourceStatement.close();
            Assertions.assertFalse(result);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }


    @Test
    @Order(4)
    public void testQueryingSomeBids() {
        try {
            String query = " SELECT * FROM bids_source LIMIT 10;";
            Statement queryStatement = connection.createStatement();
            var result = queryStatement.executeQuery(query);
            while (result.next()) {
                System.out.println(result.getString(1));
            }
            System.out.println("Query executed.");
            queryStatement.close();
            Assertions.assertNotNull(result);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }


    @Test
    @Order(5)
    public void testCreateMaterializedView() {
        try {
            String createMaterializedView =
                    " CREATE MATERIALIZED VIEW IF NOT EXISTS bids_materialized_view AS" +
                            " SELECT" +
                            " (data->'actualEvent'->'actualEvent'->>'auction')::bigint AS auction," +
                            " (data->'actualEvent'->'actualEvent'->>'bidder')::bigint AS bidder," +
                            " (data->'actualEvent'->'actualEvent'->>'price')::bigint AS price," +
                            " (data->'actualEvent'->'actualEvent'->>'channel')::text AS channel," +
                            " (data->'actualEvent'->'actualEvent'->>'url')::text AS url," +
                            " (data->'actualEvent'->'actualEvent'->>'entryTime')::timestamp AS entryTime," +
                            " (data->'actualEvent'->'actualEvent'->>'extra')::text AS extra" +
                            //" (data->>'wallclockTimestamp')::bigint AS wallclockTimestamp"
                            " FROM bids_source;";
            Statement createMaterializedViewStatement = connection.createStatement();
            var result = createMaterializedViewStatement.execute(createMaterializedView);
            System.out.println("Materialized view created.");
            createMaterializedViewStatement.close();
            Assertions.assertFalse(result);

            // select 10 elements from the materialized view
            String query = " SELECT * FROM bids_materialized_view LIMIT 10;";
            Statement queryStatement = connection.createStatement();
            var queryResult = queryStatement.executeQuery(query);
            while (queryResult.next()) {
                System.out.println("Auction | Bidder | Price | Channel | Url | EntryTime | Extra");
                System.out.println(queryResult.getString(1) + " " + queryResult.getString(2)
                        + " " + queryResult.getString(3) + " " + queryResult.getString(4)
                        + " " + queryResult.getString(5) + " " + queryResult.getString(6)
                        + " " + queryResult.getString(7));
            }
            System.out.println("Query executed.");
            queryStatement.close();
            Assertions.assertNotNull(queryResult);

            String dropMaterializedView = "DROP MATERIALIZED VIEW IF EXISTS bids_materialized_view;";
            Statement dropMaterializedViewStatement = connection.createStatement();
            dropMaterializedViewStatement.execute(dropMaterializedView);
            System.out.println("Materialized view dropped.");
            dropMaterializedViewStatement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
