package it.agilelab.thesis.nexmark.model;

import java.util.Objects;


/**
 * The next event and its various timestamps. Ordered by increasing wallclock timestamp, then
 * (arbitrary but stable) event hash order.
 */
public class NextEvent implements Comparable<NextEvent> {
    /**
     * When, in wallclock time, should this event be emitted?
     */
    private long wallclockTimestamp;

    /**
     * When, in event time, should this event be considered to have occurred?
     */
    private long eventTimestamp;

    /**
     * The event itself.
     */
    private Event<?> actualEvent;

    /**
     * The minimum of this and all future event timestamps.
     */
    private long watermark;

    public NextEvent() {
    }

    public NextEvent(final long wallclockTimestamp,
                     final long eventTimestamp,
                     final Event<?> actualEvent,
                     final long watermark) {
        this.wallclockTimestamp = wallclockTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.actualEvent = actualEvent;
        this.watermark = watermark;
    }

    /**
     * Returns the wallclock timestamp.
     * <p>
     * The wallclock timestamp is the time at which the event should be emitted. It represents
     * the moment when the event is supposed to be emitted in the simulation or event stream.
     * <p>
     * The wallclock timestamp is not necessarily the same as the event timestamp.
     * <p>
     * In the Nexmark Generator, the "timestamp wallclock" represents the moment when an event
     * should be output or produced in real life, based on the current system time. It is used to determine
     * when the event should be issued in the generated event sequence.
     *
     * @return The wallclock timestamp.
     */
    public long getWallclockTimestamp() {
        return this.wallclockTimestamp;
    }

    /**
     * Sets the wallclock timestamp.
     *
     * @param wallclockTimestamp The wallclock timestamp.
     */
    public void setWallclockTimestamp(final long wallclockTimestamp) {
        this.wallclockTimestamp = wallclockTimestamp;
    }

    /**
     * Returns the event timestamp.
     * <p>
     * The event timestamp is the time at which the event occurred in the simulation. It represents
     * the moment when the event is supposed to have occurred in the simulation or event stream.
     *
     * @return The event timestamp.
     */
    public long getEventTimestamp() {
        return this.eventTimestamp;
    }

    /**
     * Sets the event timestamp.
     *
     * @param eventTimestamp The event timestamp.
     */
    public void setEventTimestamp(final long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    /**
     * Returns the event.
     *
     * @return The event.
     */
    public Event<?> getActualEvent() {
        return this.actualEvent;
    }

    /**
     * Returns this event's watermark.
     * <p>
     * The watermark indicates the time until which it is guaranteed that all previous events have been processed.
     * It can be considered as an upper limit for the arrival of "delayed" events in the system.
     * When a data processing system receives a watermark, it knows that it can output results
     * related to events prior to the watermark, as all events with a previous timestamp have been processed.
     * <p>
     * For example, if the watermark is {@code 1000}, then all events with a timestamp of {@code 999} or earlier
     * have been processed, and any event with a timestamp of {@code 1000} or later may not have been processed yet.
     * <p>
     * The watermark is monotonically increasing, i.e., newer events will never have a
     * lower watermark than older events.
     *
     * @return The watermark for this event.
     */
    public long getWatermark() {
        return this.watermark;
    }

    /**
     * Sets the watermark.
     *
     * @param watermark The watermark.
     */
    public void setWatermark(final long watermark) {
        this.watermark = watermark;
    }

    /**
     * Generated equals method, used to compare objects.
     *
     * @param o the object to compare with
     * @return true if the input object is equal to the current object
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NextEvent nextEvent = (NextEvent) o;

        return (wallclockTimestamp == nextEvent.wallclockTimestamp
                && eventTimestamp == nextEvent.eventTimestamp
                && watermark == nextEvent.watermark
                && actualEvent.equals(nextEvent.actualEvent));
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.wallclockTimestamp, this.eventTimestamp, this.watermark, this.actualEvent);
    }

    /**
     * Generated compareTo method, used to compare objects.
     *
     * @param other the object to compare with
     * @return an integer whose sign represents the comparison result
     */
    @Override
    public int compareTo(final NextEvent other) {
        int i = Long.compare(this.wallclockTimestamp, other.wallclockTimestamp);
        if (i != 0) {
            return i;
        }
        return Integer.compare(this.actualEvent.hashCode(), other.actualEvent.hashCode());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return String.format(
                "NextEvent{wallclockTimestamp:%d; eventTimestamp:%d; actualEvent:%s; watermark:%d}",
                this.wallclockTimestamp, this.eventTimestamp, this.actualEvent, this.watermark);
    }
}
