package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AuctionTest {
    private Auction auction;

    @BeforeEach
    void setup() {
        long id = 1;
        String itemName = "item1";
        String description = "description1";
        long initialBid = 10;
        long reserve = 10;
        Instant entryTime = Instant.now();
        Instant expirationTime = Instant.now();
        long seller = 1;
        long category = 1;
        String extra = "extra1";

        auction = new Auction(id, itemName, description,
                initialBid, reserve, entryTime, expirationTime, seller, category, extra);
    }

    @Test
    void getId() {
        long expectedId = 1;
        assertEquals(expectedId, auction.getId());
    }

    @Test
    void getItemName() {
        String expectedItemName = "item1";
        assertEquals(expectedItemName, auction.getItemName());
    }

    @Test
    void getDescription() {
        String expectedDescription = "description1";
        assertEquals(expectedDescription, auction.getDescription());
    }

    @Test
    void getInitialBid() {
        long expectedInitialBid = 10;
        assertEquals(expectedInitialBid, auction.getInitialBid());
    }

    @Test
    void getReserve() {
        long expectedReserve = 10;
        assertEquals(expectedReserve, auction.getReserve());
    }

    @Test
    void getEntryTime() {
        Instant expectedEntryTime = Instant.now();
        // This kind of test needs a delta, because the two instants are not exactly the same
        // since they are created at the same time, but not exactly at the same time
        assertEquals(expectedEntryTime.toEpochMilli(), auction.getEntryTime().toEpochMilli(), 100);
    }

    @Test
    void getExpirationTime() {
        Instant expectedExpirationTime = Instant.now();
        assertEquals(expectedExpirationTime.toEpochMilli(), auction.getExpirationTime().toEpochMilli(), 100);
    }

    @Test
    void getSeller() {
        long expectedSeller = 1;
        assertEquals(expectedSeller, auction.getSeller());
    }

    @Test
    void getCategory() {
        long expectedCategory = 1;
        assertEquals(expectedCategory, auction.getCategory());
    }

    @Test
    void getExtra() {
        String expectedExtra = "extra1";
        assertEquals(expectedExtra, auction.getExtra());
    }
}
