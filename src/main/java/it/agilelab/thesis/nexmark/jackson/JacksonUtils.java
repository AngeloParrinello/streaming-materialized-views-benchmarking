package it.agilelab.thesis.nexmark.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

public final class JacksonUtils {

    private JacksonUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated");
    }

    /**
     * Returns the value of the property with the given name as an instance of the given type.
     * If the property is not present or is null, null is returned.
     * <p>
     * Delegate deserialization to the deserialization context. This is useful when the type of the
     * property is not known at compile time. When I have to deserialize a type of data that
     * is not the type that I am managing in this deserializer.
     *
     * @param object the object to deserialize
     * @param name   the name of the property
     * @param type   the type of the property
     * @param <T>    the type of the property
     * @return the value of the property with the given name as an instance of the given type
     */
    public static <T> T getElementAs(final JsonNode object,
                                     final String name,
                                     final Class<T> type) throws IOException {
        if (object.has(name)) {
            JsonNode valueNode = object.get(name);
            if (valueNode.isNull()) {
                return null;
            }
            return getMapper().treeToValue(valueNode, type);
        }
        return null;
    }

    /**
     * Returns the Jackson {@link ObjectMapper} used to serialize and deserialize objects.
     *
     * @return the Jackson {@link ObjectMapper} used to serialize and deserialize objects
     */
    public static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }
}
