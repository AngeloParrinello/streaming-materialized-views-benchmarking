package it.agilelab.thesis.nexmark.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

import java.util.Objects;

public class KsqlDBConfig {
    /**
     * The host of the KsqlDB server.
     */
    private final String ksqlDBHost;
    /**
     * The port of the KsqlDB server.
     */
    private final int ksqlDBPort;

    public KsqlDBConfig(final String ksqlDBHost, final int ksqlDBPort) {
        this.ksqlDBHost = ksqlDBHost;
        this.ksqlDBPort = ksqlDBPort;
    }

    /**
     * Build a {@link Client} object to communicate with the KsqlDB server.
     *
     * @return a {@link Client} object to communicate with the KsqlDB server.
     */
    public Client build() {
        ClientOptions options = ClientOptions.create()
                .setHost(this.ksqlDBHost)
                // Only required on communication over the network
                // .setUseTls(true)
                // .setUseAlpn(true)
                .setPort(this.ksqlDBPort);

        return Client.create(options);
    }

    /**
     * Get the host of the KsqlDB server.
     *
     * @return the host of the KsqlDB server.
     */
    public String getKsqlDBHost() {
        return this.ksqlDBHost;
    }

    /**
     * Get the port of the KsqlDB server.
     *
     * @return the port of the KsqlDB server.
     */
    public int getKsqlDBPort() {
        return this.ksqlDBPort;
    }

    /**
     * Generated equals method for comparing two objects.
     *
     * @param o the object to compare with.
     * @return true if those two objects are the same.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KsqlDBConfig)) {
            return false;
        }
        KsqlDBConfig that = (KsqlDBConfig) o;
        return getKsqlDBPort() == that.getKsqlDBPort() && Objects.equals(getKsqlDBHost(), that.getKsqlDBHost());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getKsqlDBHost(), getKsqlDBPort());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "KsqlDBConfig{"
                + "ksqlDBHost='" + ksqlDBHost + '\''
                + ", ksqlDBPort=" + ksqlDBPort
                + '}';
    }
}
