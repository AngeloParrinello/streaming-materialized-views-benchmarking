package it.agilelab.thesis.nexmark.jackson.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import it.agilelab.thesis.nexmark.model.Auction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class AuctionPresentationTest {
    private ObjectMapper mapper;
    private Auction auction;

    @BeforeEach
    final void setup() {
        mapper = JacksonUtils.getMapper();
        auction = new Auction(1003, "yyw id", "mzwgrnirxohhl", 542, 2699885,
                Instant.parse("2023-06-06T17:08:18.047Z"), Instant.parse("2023-06-06T17:08:18.115Z"),
                1000, 13, "TZQPKTflutzabebfapSa^_aPN[LaPVU]P_VLKUU]NNnztbhrhxzz");
        System.out.println(auction.getEntryTime());
    }

    @Test
    void testAuctionSerialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(auction);
        System.out.println(json);
        Assertions.assertNotNull(json);
    }

    @Test
    void testAuctionDeserialization() throws JsonProcessingException {
        String json = mapper.writeValueAsString(auction);
        Auction auction = mapper.readValue(json, Auction.class);
        Assertions.assertEquals(this.auction, auction);
    }

}
