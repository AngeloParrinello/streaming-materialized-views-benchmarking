package it.agilelab.thesis.nexmark.jackson.model;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class EventPresentationTest {
    private ObjectMapper mapper;
    private Event<Auction> event;

    @BeforeEach
    final void setup() {
        mapper = JacksonUtils.getMapper();
        Auction auction = new Auction(1001, "las", "uvcvkpzmnhwex buyyrqmaipzapfvydaiz",
                1599496, 1847664, Instant.parse("2023-06-07T08:04:15.949Z"),
                Instant.parse("2023-06-07T08:04:16.126Z"), 1005, 14,
                "tagsRN^JLR___^[Hxlfejfixjwxq[UM^L_utxptzaJQK__[[should__]KP");
        event = new Event<>(auction);
        System.out.println(event);
    }

    @Test
    void testEventSerialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(event);
        System.out.println(json);
        Assertions.assertNotNull(json);
    }

    @Test
    void testEventDeserialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(event);
        var event = mapper.readValue(json, Event.class);
        Assertions.assertEquals(this.event, event);
    }
}
