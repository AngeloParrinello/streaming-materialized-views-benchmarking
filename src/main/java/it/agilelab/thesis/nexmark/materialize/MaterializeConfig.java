package it.agilelab.thesis.nexmark.materialize;

import it.agilelab.thesis.nexmark.NexmarkUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public final class MaterializeConfig {
    private static final String URL = "jdbc:postgresql://localhost:6875/materialize";
    private static final String USER = "materialize";
    private static final String PASSWORD = "materialize";

    private MaterializeConfig() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Connect to the Materialize database.
     *
     * @return a Connection object
     * @throws SQLException if a database access error occurs
     */
    public static Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("ssl", "false");

        Connection conn = DriverManager.getConnection(URL, props);
        NexmarkUtil.getLogger().info("Connected to Materialize successfully!");
        return conn;
    }

}
