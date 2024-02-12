package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventTest {

    @Test
    void testNewPersonEvent() {
        Person person = createPerson();
        Event<Person> event = new Event<>(person);
        assertEventState(event, person, EventType.PERSON);
    }

    @Test
    void testNewAuctionEvent() {
        Auction auction = createAuction();
        Event<Auction> event = new Event<>(auction);
        assertEventState(event, auction, EventType.AUCTION);
    }

    @Test
    void testBidEvent() {
        Bid bid = createBid();
        Event<Bid> event = new Event<>(bid);
        assertEventState(event, bid, EventType.BID);
    }

    @Test
    void testGetActualEvent() {
        Bid bid = createBid();
        Event<Bid> event = new Event<>(bid);
        assertEquals(bid, event.getActualEvent());
    }

    private Person createPerson() {
        return new Person(1, "John Doe", "john.doe@example.com",
                "1234567890", "New York", "NY", Instant.now(), "Some extra data");
    }

    private Auction createAuction() {
        return new Auction(1, "Auction 1", "description", 10, 1, Instant.now(),
                Instant.now(), 1, 1, "Some extra data");
    }

    private Bid createBid() {
        return new Bid(1, 1, 10, "abc123",
                "url123", Instant.now(), "Some extra data");
    }

    private <T> void assertEventState(final Event<T> event,
                                      final T expectedEvent,
                                      final EventType expectedEventType) {
        assertEquals(expectedEvent, event.getActualEvent());
        assertEquals(expectedEventType, event.getEventType());
    }
}
