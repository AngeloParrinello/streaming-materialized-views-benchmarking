package it.agilelab.thesis.nexmark.jackson.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Event;
import it.agilelab.thesis.nexmark.model.NextEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;


public class NextEventPresentationTest {
    private ObjectMapper mapper;
    private NextEvent nextEvent;

    @BeforeEach
    final void setup() {
        mapper = JacksonUtils.getMapper();
        Auction auction = new Auction(1002, "path", "rtilrgtaounvjcl eta",
                3935973, 26690626, Instant.parse("2023-06-07T08:10:11.035Z"),
                Instant.parse("2023-06-07T08:10:11.149Z"), 1000, 13,
                "V``KJMrjobRJ_QMIbfpxswlgdiydWRMM[KOZO^YO_PHPKbrbzhxdhrebrvitphfU]RZHSeaborg");
        Event<Auction> event = new Event<>(auction);
        nextEvent = new NextEvent(1686125411035L, 1686125411035L, event, 1686125411035L);
    }

    @Test
    void testNextEventSerialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(nextEvent);
        System.out.println(json);
        Assertions.assertNotNull(json);
    }

    @Test
    void testNextEventDeserialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(nextEvent);
        System.out.println(json);
        NextEvent nextEvent = mapper.readValue(json, NextEvent.class);
        System.out.println(nextEvent);
        Assertions.assertEquals(this.nextEvent, nextEvent);
    }

}
