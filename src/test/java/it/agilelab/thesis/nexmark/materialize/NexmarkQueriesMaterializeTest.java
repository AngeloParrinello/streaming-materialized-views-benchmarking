package it.agilelab.thesis.nexmark.materialize;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.kafka.NexmarkKafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.DisabledIf;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * For the moment, this test can only be run locally. So, first of all, you need to start the Kafka container with,
 * for example, the following command:
 * <p>
 *     docker compose up -d --remove-orphans
 *
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledIf(value = "it.agilelab.thesis.nexmark.NexmarkTestsUtil#isKafkaContainerNotRunning", disabledReason = "Local test")
@Testcontainers(disabledWithoutDocker = true, parallel = true)
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
public class NexmarkQueriesMaterializeTest {
    private static Connection connection;

    @BeforeAll
    public static void setup() throws SQLException, IOException {
        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, 1000, 1);

        NexmarkKafkaProducer nexmarkProducer = new NexmarkKafkaProducer("localhost:29092",
                config, 100);
        nexmarkProducer.start();

        connection = MaterializeConfig.connect();

        connectToKafka();
    }

    @Test
    void query1Test() throws IOException, SQLException {
        List<String> query1 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query1.sql");
        query1.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query1Result = "SELECT * FROM nexmark_q1 limit 10;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query1Result);
        assertNotNull(queryResult);
        System.out.println("Auction | Bidder | Price | EntryTime | Extra");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2)
                    + " " + queryResult.getString(3) + " " + queryResult.getString(4)
                    + " " + queryResult.getString(5));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query2Test() throws SQLException, IOException {
        List<String> query2 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query2.sql");
        query2.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query2Result = "SELECT * FROM nexmark_q2;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query2Result);
        assertNotNull(queryResult);
        System.out.println("Auction | Price");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query3Test() throws SQLException, IOException {
        List<String> query3 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query3.sql");
        query3.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query3Result = "SELECT * FROM nexmark_q3;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query3Result);
        assertNotNull(queryResult);
        System.out.println("Name | City | State | Id");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2)
                    + " " + queryResult.getString(3) + " " + queryResult.getString(4));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query4Test() throws SQLException, IOException {
        List<String> query4 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query4.sql");
        query4.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query4Result = "SELECT * FROM nexmark_q4;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query4Result);
        assertNotNull(queryResult);
        System.out.println("Category | Avg");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query5Test() throws SQLException, IOException {
        List<String> query5 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query5.sql");
        query5.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query5Result = "SELECT * FROM nexmark_q5;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query5Result);
        assertNotNull(queryResult);
        System.out.println("Auction | Num | Start | End");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2)
                    + " " + queryResult.getString(3) + " " + queryResult.getString(4));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query6Test() throws SQLException, IOException {
        List<String> query6 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query6.sql");
        query6.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query6Result = "SELECT * FROM nexmark_q6;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query6Result);
        assertNotNull(queryResult);
        System.out.println("Seller | Avg");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query7Test() throws SQLException, IOException {
        List<String> query7 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query7.sql");
        query7.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query7Result = "SELECT * FROM nexmark_q7;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query7Result);
        assertNotNull(queryResult);
        System.out.println("Seller | Avg");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    @Test
    void query8Test() throws SQLException, IOException {
        List<String> query8 = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/query8.sql");
        query8.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        String query8Result = "SELECT * FROM nexmark_q8;";
        Statement queryStatement = connection.createStatement();
        ResultSet queryResult = queryStatement.executeQuery(query8Result);
        assertNotNull(queryResult);
        System.out.println("Id | Name | Start | End");
        while (queryResult.next()) {
            System.out.println(queryResult.getString(1) + " " + queryResult.getString(2)
                    + " " + queryResult.getString(3) + " " + queryResult.getString(4));
        }
        queryStatement.close();
        System.out.println("Query executed.");
    }

    private static void connectToKafka() throws IOException {
        List<String> createConnection = NexmarkUtil.readQueryFileAsString("src/main/resources/queries/materialize/create_connection.sql");
        createConnection.forEach(statement -> {
            try {
                executeStatement(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private static void executeStatement(String statement) throws SQLException {
        Statement createConnectionStatement = connection.createStatement();
        createConnectionStatement.execute(statement);
        System.out.println("Statement executed.");
        createConnectionStatement.close();
    }


}
