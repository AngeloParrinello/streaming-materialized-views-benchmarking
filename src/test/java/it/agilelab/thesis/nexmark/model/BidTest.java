package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BidTest {
    private Bid bid;

    @BeforeEach
    void setup() {
        long auction = 1;
        long bidder = 1;
        long price = 10;
        String channel = "abc123";
        String url = "url123";
        Instant entryTime = Instant.now();
        String extra = "Some extra data";

        bid = new Bid(auction, bidder, price, channel, url, entryTime, extra);
    }

    @Test
    void getAuction() {
        long expectedAuction = 1;
        assertEquals(expectedAuction, bid.getAuction());
    }

    @Test
    void getBidder() {
        long expectedBidder = 1;
        assertEquals(expectedBidder, bid.getBidder());
    }

    @Test
    void getPrice() {
        long expectedPrice = 10;
        assertEquals(expectedPrice, bid.getPrice());
    }

    @Test
    void getChannel() {
        String expectedChannel = "abc123";
        assertEquals(expectedChannel, bid.getChannel());
    }

    @Test
    void getUrl() {
        String expectedUrl = "url123";
        assertEquals(expectedUrl, bid.getUrl());
    }

    @Test
    void getEntryTime() {
        Instant expectedEntryTime = Instant.now();
        // This kind of test needs a delta, because the two instants are not exactly the same
        // since they are created at the same time, but not exactly at the same time
        assertEquals(expectedEntryTime.toEpochMilli(), bid.getEntryTime().toEpochMilli(), 100);
    }

    @Test
    void getExtra() {
        String expectedExtra = "Some extra data";
        assertEquals(expectedExtra, bid.getExtra());
    }
}
