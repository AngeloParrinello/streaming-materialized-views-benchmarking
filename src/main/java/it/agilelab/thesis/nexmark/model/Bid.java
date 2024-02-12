package it.agilelab.thesis.nexmark.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A bid for an item on auction.
 */
public class Bid implements Serializable {
    /**
     * Id of auction this bid is for.
     */
    private long auction; // foreign key: Auction.id

    /**
     * Id of person bidding in auction.
     */
    private long bidder; // foreign key: Person.id

    /**
     * Price of bid, in cents.
     */
    private long price;

    /**
     * The channel introduced this bidding.
     */
    private String channel;

    /**
     * The url of this bid.
     */
    private String url;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
    private Instant entryTime;

    /**
     * Additional arbitrary payload for performance testing.
     */
    private String extra;

    public Bid() {
    }

    public Bid(final long auction,
               final long bidder,
               final long price,
               final String channel,
               final String url,
               final Instant entryTime,
               final String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.entryTime = entryTime;
        this.extra = extra;
    }

    /**
     * Get the id of the auction this bid is for.
     *
     * @return the auction's id
     */
    public long getAuction() {
        return this.auction;
    }

    /**
     * Set the id of the auction this bid is for.
     *
     * @param auction the auction's id
     */
    public void setAuction(final long auction) {
        this.auction = auction;
    }

    /**
     * Get the id of the person bidding in auction.
     *
     * @return the bidder's id
     */
    public long getBidder() {
        return this.bidder;
    }

    /**
     * Set the id of the person bidding in auction.
     *
     * @param bidder the bidder's id
     */
    public void setBidder(final long bidder) {
        this.bidder = bidder;
    }

    /**
     * Get the price of the bid, in cents.
     *
     * @return the bid's price
     */
    public long getPrice() {
        return this.price;
    }

    /**
     * Set the price of the bid.
     *
     * @param price the bid's price
     */
    public void setPrice(final long price) {
        this.price = price;
    }

    /**
     * Get the channel introduced this bidding.
     *
     * @return the channel's string id
     */
    public String getChannel() {
        return this.channel;
    }

    /**
     * Set the channel introduced this bidding.
     *
     * @param channel the channel's string id
     */
    public void setChannel(final String channel) {
        this.channel = channel;
    }

    /**
     * Get the url of this bid.
     *
     * @return the string (url) of the bid
     */
    public String getUrl() {
        return this.url;
    }

    /**
     * Set the url of this bid.
     *
     * @param url the string (url) of the bid
     */
    public void setUrl(final String url) {
        this.url = url;
    }

    /**
     * Get the instant of when the data has been submitted into the system.
     *
     * @return the bid's entry time
     */
    public Instant getEntryTime() {
        return this.entryTime;
    }

    /**
     * Set the instant of when the data has been submitted into the system.
     *
     * @param entryTime the bid's entry time
     */
    public void setEntryTime(final Instant entryTime) {
        this.entryTime = entryTime;
    }

    /**
     * Get some extra data linked to that bid.
     *
     * @return the bid's extra data
     */
    public String getExtra() {
        return this.extra;
    }

    /**
     * Set some extra data linked to that bid.
     *
     * @param extra the bid's extra data
     */
    public void setExtra(final String extra) {
        this.extra = extra;
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
        if (!(o instanceof Bid)) {
            return false;
        }
        Bid bid = (Bid) o;
        return getAuction() == bid.getAuction() && getBidder() == bid.getBidder()
                && getPrice() == bid.getPrice() && Objects.equals(getChannel(), bid.getChannel())
                && Objects.equals(getUrl(), bid.getUrl()) && Objects.equals(getEntryTime(), bid.getEntryTime())
                && Objects.equals(getExtra(), bid.getExtra());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getAuction(), getBidder(), getPrice(), getChannel(),
                getUrl(), getEntryTime(), getExtra());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "Bid{"
                + "auction=" + this.auction
                + ", bidder=" + this.bidder
                + ", price=" + this.price
                + ", channel='" + this.channel + '\''
                + ", url='" + this.url + '\''
                + ", entryTime=" + this.entryTime
                + ", extra='" + this.extra + '\''
                + '}';
    }
}
