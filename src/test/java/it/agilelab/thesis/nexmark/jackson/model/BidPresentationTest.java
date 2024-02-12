package it.agilelab.thesis.nexmark.jackson.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.Bid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class BidPresentationTest {
    private ObjectMapper mapper;
    private Bid bid;

    @BeforeEach
    final void setup() {
        mapper = JacksonUtils.getMapper();
        bid = new Bid(1000, 1004, 74857254, "channel-892",
                "https://www.nexmark.com/zhq/okdm/didh/item.htm?query=1&channel_id=1052770304",
                Instant.parse("2023-06-06T16:32:54.403Z"),
                "LZ`]_TxvmosckkbssdHO]THQRQZSVTnokybeumbjysglbxovJUQPT_pinoJO");
    }

    @Test
    void testBidSerialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(bid);
        System.out.println(json);
        Assertions.assertNotNull(json);
    }

    @Test
    void testBidDeserialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(bid);
        Bid bid = mapper.readValue(json, Bid.class);
        Assertions.assertEquals(this.bid, bid);
    }

}
