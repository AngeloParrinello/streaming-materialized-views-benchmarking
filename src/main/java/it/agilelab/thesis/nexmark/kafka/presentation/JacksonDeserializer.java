package it.agilelab.thesis.nexmark.kafka.presentation;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class JacksonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;
    private Class<T> clazz;

    public JacksonDeserializer() {
        this.objectMapper = JacksonUtils.getMapper();
    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // intentionally left blank because no further configuration is needed
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return this.objectMapper.readValue(data, this.clazz);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Close this deserializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() {
        Deserializer.super.close();
    }

    /**
     * Get the class of the object to be deserialized.
     *
     * @return the class
     */
    public Class<T> getClazz() {
        return this.clazz;
    }

    /**
     * Set the class of the object to be deserialized.
     *
     * @param clazz the class
     */
    public void setClazz(final Class<T> clazz) {
        this.clazz = clazz;
    }

    /**
     * Get the object mapper.
     *
     * @return the object mapper
     */
    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
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
        if (!(o instanceof JacksonDeserializer)) {
            return false;
        }
        JacksonDeserializer<?> that = (JacksonDeserializer<?>) o;
        return Objects.equals(getClazz(), that.getClazz()) && Objects.equals(getObjectMapper(),
                that.getObjectMapper());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getClazz(), getObjectMapper());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "JacksonDeserializer{"
                + "clazz=" + this.clazz
                + ", objectMapper=" + this.objectMapper
                + '}';
    }
}
