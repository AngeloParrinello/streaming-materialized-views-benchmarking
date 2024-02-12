package it.agilelab.thesis.nexmark;

import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.flink.source.NexmarkGeneratorSource;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/**
 * This class presents some utility methods for tests in Nexmark.
 */
public class NexmarkTestsUtil {

    private static final Logger LOGGER = LogManager.getLogger(NexmarkTestsUtil.class);
    private final int ksqlDbPort = 8088;
    private final Network network;
    private KafkaContainer kafka;
    private String kafkaBootstrapServers;
    private GenericContainer<?> ksqlDB;

    public NexmarkTestsUtil() {
        this.network = Network.newNetwork();
    }

    /**
     * Check if a container is not running on port 29092.
     */
    public static boolean isKafkaContainerNotRunning() {
        try {
            Socket socket = new Socket("localhost", 29092);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    /**
     * Set a custom log4j2 configuration file, which is used to initialize Log4j.
     * <p>
     * This method is helpful when you want to use a custom log4j2 configuration file for your tests.
     *
     * @param log4j2ConfigFile the custom log4j2 configuration file.
     */
    public void useCustomLog4j2Config(final File log4j2ConfigFile) {
        // Initialize Log4j
        System.setProperty("log4j.configurationFile", log4j2ConfigFile.toURI().toString());

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(log4j2ConfigFile.toURI());
    }

    /**
     * This method starts a Kafka container.
     * <p>
     * The method starts a new Kafka temporary container and set the bootstrap servers to the new container.
     */
    public void startKafkaContainer() {
        this.kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withNetwork(this.network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .waitingFor(Wait.forHttp("/info").forStatusCode(200));

        this.kafka.start();
        assert this.kafka.isRunning();
        this.kafkaBootstrapServers = this.kafka.getHost() + ":" + this.kafka.getFirstMappedPort();
    }

    /**
     * This method starts a KsqlDB container.
     */
    public void startKsqlContainer() {
        if (this.kafka == null) {
            this.startKafkaContainer();
        }

        this.ksqlDB = new GenericContainer<>("confluentinc/ksqldb-server:0.29.0")
                .withExposedPorts(this.ksqlDbPort)
                .dependsOn(this.kafka)
                .withNetwork(this.network)
                .withNetworkAliases("ksql-server")
                .withEnv("HOST_NAME", "ksql-server")
                .withEnv("KSQL_CACHE_MAX_BYTES_BUFFERING", "0")
                .withEnv("KSQL_LISTENERS", "http://0.0.0.0:8088")
                .withEnv("KSQL_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE", "true")
                .withEnv("KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE", "true")
                .waitingFor(Wait.forHttp("/info").forStatusCode(200));

        this.ksqlDB.start();
        assert this.ksqlDB.isRunning();
    }

    /**
     * This method returns the Kafka container.
     *
     * @return the Kafka container.
     */
    public KafkaContainer getKafka() {
        return this.kafka;
    }

    /**
     * This method returns the KsqlDB container.
     *
     * @return the KsqlDB container.
     */
    public GenericContainer<?> getKsqlDB() {
        return this.ksqlDB;
    }

    /**
     * This method returns the host of the KsqlDB container.
     *
     * @return the host of the KsqlDB container.
     */
    public String getKsqlDBHost() {
        return this.ksqlDB.getHost();
    }

    /**
     * This method returns the port of the KsqlDB container.
     *
     * @return the port of the KsqlDB container.
     */
    public int getKsqlDBPort() {
        return this.getKsqlDB().getMappedPort(this.ksqlDbPort);
    }

    /**
     * This method returns the bootstrap servers of the Kafka container.
     *
     * @return the bootstrap servers of the Kafka container.
     */
    public String getKafkaBootstrapServers() {
        return this.kafkaBootstrapServers;
    }

    /**
     * Get the log4j2 logger.
     *
     * @return the log4j2 logger.
     */
    public Logger getLogger() {
        return LOGGER;
    }

    /**
     * Get the network used by the containers.
     *
     * @return the network.
     */
    public Network getNetwork() {
        return network;
    }

    /**
     * This method stops the Kafka container.
     */
    public void stopKafkaContainer() {
        if (this.kafka != null) {
            this.kafka.stop();
        }

        this.kafka = null;
    }

    /**
     * This method stops the KsqlDB container.
     */
    public void stopKsqlContainer() {
        if (this.ksqlDB != null) {
            this.ksqlDB.stop();
        }

        this.ksqlDB = null;
    }

    /**
     * This method stops the containers.
     */
    public void stopContainers() {
        this.stopKsqlContainer();
        this.stopKafkaContainer();
    }

    /**
     * This method creates some events on Kafka.
     *
     * @param numberOfEvents the number of events to create.
     * @throws Exception if something goes wrong during the creation of the events.
     */
    public void createSomeEventsOnKafka(final long numberOfEvents) throws Exception {

        GeneratorConfig config = new GeneratorConfig(new NexmarkConfiguration(),
                System.currentTimeMillis(), 1, numberOfEvents, 1);

        NexmarkGeneratorSource flinkProducer = new NexmarkGeneratorSource(config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<NextEvent> stream = env.addSource(flinkProducer);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setBootstrapServers("localhost:29092")
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        stream.sinkTo(sink);
        env.execute();
    }
}
