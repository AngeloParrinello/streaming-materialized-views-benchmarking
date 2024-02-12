package it.agilelab.thesis.nexmark.flink.jobs;

import it.agilelab.thesis.nexmark.NexmarkConfiguration;
import it.agilelab.thesis.nexmark.YamlParser;
import it.agilelab.thesis.nexmark.flink.presentation.NextEventKafkaSerializationSchema;
import it.agilelab.thesis.nexmark.flink.source.NexmarkGeneratorSource;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.util.Properties;

public final class EventsOnKafkaJob {
    private EventsOnKafkaJob() {
        throw new IllegalStateException("This class will be used as a Flink's job and it should not be instantiated");
    }

    public static void main(final String[] args) throws Exception {
        NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
        ParameterTool parameters = ParameterTool.fromArgs(args);

        int parallelism = parameters.getInt("parallelism", 8);

        YamlParser yamlParser = new YamlParser();
        yamlParser.parse("config/nexmark-conf.yaml");

        int numberEvents = yamlParser.getValue("nexmark.number.events");
        String bootstrapServers = yamlParser.getValue("nexmark.kafka.bootstrap.servers");
        String deliveryGuarantee = yamlParser.getValue("nexmark.kafka.delivery.guarantee");
        String kafkaPropertiesPath = yamlParser.getValue("nexmark.kafka.producer.properties.path");
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty("transaction.timeout.ms", "60000");
        /* In case you have some problems with the delivery of the events, try to increase the request timeout
            Uncomment the following lines and change the value as you prefer. For example,
            during the test with the parallelism set to 80, the cluster Kafka was not able to handle the requests
            due to its configuration, so I had to increase the timeout on the producer side instead
            of increase the cluster's power. The following configuration solved the problem.
        kafkaProducerProperties.setProperty("request.timeout.ms", "60000");
        kafkaProducerProperties.setProperty("delivery.timeout.ms", "180000");
        kafkaProducerProperties.setProperty("batch.size", "100000");
        kafkaProducerProperties.setProperty("linger.ms", "100");
        */
        if (kafkaPropertiesPath != null) {
            kafkaProducerProperties.load(new FileInputStream(kafkaPropertiesPath));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Must restart a job from a checkpoint to avoid duplicate events
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConfiguration, System.currentTimeMillis(), 1, numberEvents, 1);

        NexmarkGeneratorSource source = new NexmarkGeneratorSource(generatorConfig);

        KafkaSink<NextEvent> sink = KafkaSink.<NextEvent>builder()
                .setKafkaProducerConfig(kafkaProducerProperties)
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new NextEventKafkaSerializationSchema())
                .setDeliveryGuarantee(deliveryGuarantee.equals("at_least_once") ? DeliveryGuarantee.AT_LEAST_ONCE : DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        env.addSource(source)
                .sinkTo(sink)
                .name("NexmarkKafkaSink");

        env.execute("Nexmark Generator");
    }
}
