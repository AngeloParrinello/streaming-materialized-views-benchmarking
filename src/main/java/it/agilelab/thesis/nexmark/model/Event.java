package it.agilelab.thesis.nexmark.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import it.agilelab.thesis.nexmark.jackson.model.EventDeserializer;

import java.util.Objects;

@JsonDeserialize(using = EventDeserializer.class)
public class Event<T> {

    private T actualEvent;
    private EventType eventType;

    public Event() {
    }

    public Event(final T actualEvent) {
        this.actualEvent = actualEvent;
        this.eventType = resolveEventType(actualEvent);
    }

    /**
     * Get the actual event.
     *
     * @return the actual event
     */
    public T getActualEvent() {
        return this.actualEvent;
    }

    /**
     * Get the {@link EventType} of the event.
     *
     * @return the type of the event
     */
    public EventType getEventType() {
        return this.eventType;
    }

    private EventType resolveEventType(final T event) {
        if (event instanceof Person) {
            return EventType.PERSON;
        } else if (event instanceof Auction) {
            return EventType.AUCTION;
        } else if (event instanceof Bid) {
            return EventType.BID;
        }
        throw new IllegalStateException("Unknown event type: " + this.eventType);
    }

    /**
     * Generated equals method for comparing two objects.
     *
     * @param o the object to compare with.
     * @return true if those two objects are the same.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Event)) {
            return false;
        }
        Event<?> event1 = (Event<?>) o;
        return Objects.equals(getActualEvent(), event1.getActualEvent()) && getEventType() == event1.getEventType();
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getActualEvent(), getEventType());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "Event{"
                + "actualEvent=" + actualEvent
                + ", eventType=" + eventType
                + '}';
    }
}
