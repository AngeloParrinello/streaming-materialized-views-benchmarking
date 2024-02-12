package it.agilelab.thesis.nexmark.jackson.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.generator.GeneratorConfig;
import it.agilelab.thesis.nexmark.generator.NexmarkGenerator;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.NextEvent;

/**
 * A {@link NexmarkGenerator} that generates JSON strings.
 */
public class NexmarkJsonGenerator extends NexmarkGenerator {
    private static final ObjectMapper MAPPER = JacksonUtils.getMapper();

    /**
     * Create a brand-new fresh generator according to {@code config}.
     *
     * @param config the configuration to use
     */
    public NexmarkJsonGenerator(final GeneratorConfig config) {
        super(config);
    }

    /**
     * Calls {@link NexmarkGenerator#next()}} and serializes the result to JSON.
     *
     * @return the next event as a JSON string
     */
    public String nextJsonEvent() throws JsonProcessingException {
        NextEvent nextEvent = super.next();
        return MAPPER.writeValueAsString(nextEvent);
    }
}
