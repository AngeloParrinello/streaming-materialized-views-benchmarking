package it.agilelab.thesis.nexmark.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface KafkaProducerApplication<V> {
    /**
     * Produce an event to Kafka.
     * <p>
     * Process the event and send it to the correct broker and topic.
     *
     * @param event the event to produce
     */
    Future<RecordMetadata> produce(V event);

    /**
     * Shutdown the producer.
     */
    void shutdown();
}
