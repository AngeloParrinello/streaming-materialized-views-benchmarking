package it.agilelab.thesis.nexmark.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * An auction submitted by a person.
 */
public class Auction implements Serializable {

    /**
     * Id of auction.
     */
    private long id; // primary key

    /**
     * Extra auction properties.
     */
    private String itemName;

    private String description;

    /**
     * Initial bid price, in cents.
     */
    private long initialBid;

    /**
     * Reserve price, in cents.
     */
    private long reserve;

    private Instant entryTime;

    /**
     * When does auction expire? (ms since epoch). Bids at or after this time are ignored.
     */
    private Instant expirationTime;

    /**
     * Id of person who instigated auction.
     */
    private long seller; // foreign key: Person.id

    /**
     * Id of category auction is listed under.
     */
    private long category; // foreign key: Category.id

    /**
     * Additional arbitrary payload for performance testing.
     */
    private String extra;

    public Auction() {
    }

    public Auction(final long id,
                   final String itemName,
                   final String description,
                   final long initialBid,
                   final long reserve,
                   final Instant entryTime,
                   final Instant expirationTime,
                   final long seller,
                   final long category,
                   final String extra) {
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.entryTime = entryTime;
        this.expirationTime = expirationTime;
        this.seller = seller;
        this.category = category;
        this.extra = extra;
    }

    /**
     * Get the id of the auction.
     *
     * @return the auction's id
     */
    public long getId() {
        return id;
    }

    /**
     * Set the auction's id.
     *
     * @param id the auction's id
     */
    public void setId(final long id) {
        this.id = id;
    }

    /**
     * Get the auction's item name.
     *
     * @return the auction's item name
     */
    public String getItemName() {
        return itemName;
    }

    /**
     * Set the auction's item name.
     *
     * @param itemName the auction's item name
     */
    public void setItemName(final String itemName) {
        this.itemName = itemName;
    }

    /**
     * Get the auction's description.
     *
     * @return the auction's description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set the auction's description.
     *
     * @param description the auction's description
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * Get the auction's initial bid.
     *
     * @return the auction's initial bid
     */
    public long getInitialBid() {
        return initialBid;
    }

    /**
     * Set the auction's initial bid.
     *
     * @param initialBid the auction's initial bid
     */
    public void setInitialBid(final long initialBid) {
        this.initialBid = initialBid;
    }

    /**
     * Get the auction's reserve.
     *
     * @return the auction's reserve
     */
    public long getReserve() {
        return reserve;
    }

    /**
     * Set the auction's reserve.
     *
     * @param reserve the auction's reserve
     */
    public void setReserve(final long reserve) {
        this.reserve = reserve;
    }

    /**
     * Get the auction's entry time.
     *
     * @return the auction's entry time
     */
    public Instant getEntryTime() {
        return entryTime;
    }

    /**
     * Set the auction's entry time.
     *
     * @param entryTime the auction's entry time
     */
    public void setEntryTime(final Instant entryTime) {
        this.entryTime = entryTime;
    }

    /**
     * Get the auction's expiration date.
     *
     * @return the auction's expiration date
     */
    public Instant getExpirationTime() {
        return expirationTime;
    }

    /**
     * Set the auction's expiration date.
     *
     * @param expirationTime the auction's expiration date
     */
    public void setExpirationTime(final Instant expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * Get the auction seller's id.
     *
     * @return the auction seller id.
     */
    public long getSeller() {
        return seller;
    }

    /**
     * Set the auction's seller id.
     *
     * @param seller the auction's seller id
     */
    public void setSeller(final long seller) {
        this.seller = seller;
    }

    /**
     * Get the auction's category.
     *
     * @return the auction's category
     */
    public long getCategory() {
        return category;
    }

    /**
     * Set the auction's category.
     *
     * @param category the auction's category
     */
    public void setCategory(final long category) {
        this.category = category;
    }

    /**
     * Get the auction's extra data.
     *
     * @return the auction's extra data
     */
    public String getExtra() {
        return extra;
    }

    /**
     * Set the auction's extra data.
     *
     * @param extra the auction's extra data
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
        if (!(o instanceof Auction)) {
            return false;
        }
        Auction auction = (Auction) o;
        return getId() == auction.getId() && getInitialBid() == auction.getInitialBid()
                && getReserve() == auction.getReserve() && getSeller() == auction.getSeller()
                && getCategory() == auction.getCategory() && Objects.equals(getItemName(), auction.getItemName())
                && Objects.equals(getDescription(), auction.getDescription())
                && Objects.equals(getEntryTime(), auction.getEntryTime())
                && Objects.equals(getExpirationTime(), auction.getExpirationTime())
                && Objects.equals(getExtra(), auction.getExtra());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getItemName(), getDescription(),
                getInitialBid(), getReserve(), getEntryTime(), getExpirationTime(),
                getSeller(), getCategory(), getExtra());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "Auction{"
                + "id=" + id
                + ", itemName='" + itemName + '\''
                + ", description='" + description + '\''
                + ", initialBid=" + initialBid
                + ", reserve=" + reserve
                + ", entryTime=" + entryTime
                + ", expirationTime=" + expirationTime
                + ", seller=" + seller
                + ", category=" + category
                + ", extra='" + extra + '\''
                + '}';
    }
}
