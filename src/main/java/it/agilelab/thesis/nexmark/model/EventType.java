package it.agilelab.thesis.nexmark.model;

/**
 * The type of object stored in this event.
 */
public enum EventType {
    /**
     * Person type.
     */
    PERSON(0),
    /**
     * Auction type.
     */
    AUCTION(1),
    /**
     * Bid type.
     */
    BID(2);

    private final int type;

    EventType(final int type) {
        this.type = type;
    }

    /**
     * Get the type of the event.
     *
     * @return the event's type
     */
    public int getType() {
        return type;
    }
}
