package it.agilelab.thesis.nexmark.flink.presentation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class NextEventKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<NextEvent> {
    private transient ObjectMapper mapper;

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    @Override
    public void open(final DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
        this.mapper = JacksonUtils.getMapper();
    }

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param kafkaRecord The ConsumerRecord to deserialize.
     * @param out         The collector to put the resulting messages.
     */
    @Override
    public void deserialize(final ConsumerRecord<byte[], byte[]> kafkaRecord,
                            final Collector<NextEvent> out) throws JsonProcessingException {
        if (this.mapper == null) {
            this.mapper = JacksonUtils.getMapper();
        }

        String nextEventJson = new String(kafkaRecord.value(), StandardCharsets.UTF_8);
        out.collect(this.mapper.readValue(nextEventJson, NextEvent.class));
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<NextEvent> getProducedType() {
        return TypeInformation.of(NextEvent.class);
    }
}
