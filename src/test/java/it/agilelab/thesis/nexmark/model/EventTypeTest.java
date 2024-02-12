package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventTypeTest {

    @Test
    void testPersonType() {
        int expectedType = 0;
        assertEquals(expectedType, EventType.PERSON.getType());
    }

    @Test
    void testAuctionType() {
        int expectedType = 1;
        assertEquals(expectedType, EventType.AUCTION.getType());
    }

    @Test
    void testBidType() {
        int expectedType = 2;
        assertEquals(expectedType, EventType.BID.getType());
    }
}
