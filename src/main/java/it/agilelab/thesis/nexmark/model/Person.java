package it.agilelab.thesis.nexmark.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A person either creating an auction or making a bid.
 */
public class Person implements Serializable {

    /**
     * Id of person.
     */
    private long id; // primary key

    /**
     * Extra person properties.
     */
    private String name;

    private String emailAddress;

    private String creditCard;

    private String city;

    private String state;

    private Instant entryTime;

    /**
     * Additional arbitrary payload for performance testing.
     */
    private String extra;

    public Person() {
    }

    public Person(
            final long id,
            final String name,
            final String emailAddress,
            final String creditCard,
            final String city,
            final String state,
            final Instant entryTime,
            final String extra) {
        this.id = id;
        this.name = name;
        this.emailAddress = emailAddress;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.entryTime = entryTime;
        this.extra = extra;
    }

    /**
     * Get the person's id.
     *
     * @return the person's id
     */
    public long getId() {
        return id;
    }

    /**
     * Set the person's id.
     *
     * @param id the person's id
     */
    public void setId(final long id) {
        this.id = id;
    }

    /**
     * Get the person's name.
     *
     * @return the person's name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the person's name.
     *
     * @param name the person's name
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Get the person's email.
     *
     * @return the person's email
     */
    public String getEmailAddress() {
        return emailAddress;
    }

    /**
     * Set the person's email.
     *
     * @param emailAddress the person's email
     */
    public void setEmailAddress(final String emailAddress) {
        this.emailAddress = emailAddress;
    }

    /**
     * Get the person's credit card.
     *
     * @return the person's credit card
     */
    public String getCreditCard() {
        return creditCard;
    }

    /**
     * Set the person's credit card.
     *
     * @param creditCard the person's credit card
     */
    public void setCreditCard(final String creditCard) {
        this.creditCard = creditCard;
    }

    /**
     * Get the person's city.
     *
     * @return the person's city
     */
    public String getCity() {
        return city;
    }

    /**
     * Set the person's city.
     *
     * @param city the person's city
     */
    public void setCity(final String city) {
        this.city = city;
    }

    /**
     * Get the person's state.
     *
     * @return the person's state
     */
    public String getState() {
        return state;
    }

    /**
     * Set the person's state.
     *
     * @param state the person's state
     */
    public void setState(final String state) {
        this.state = state;
    }

    /**
     * Get the instant of when a person has been inserted into the system.
     *
     * @return the person's entry time
     */
    public Instant getEntryTime() {
        return entryTime;
    }

    /**
     * Set the instant of when a person has been inserted into the system.
     *
     * @param entryTime the instant of when a person has been inserted into the system
     */
    public void setEntryTime(final Instant entryTime) {
        this.entryTime = entryTime;
    }

    /**
     * Get some extra data linked to that person.
     *
     * @return the person's extra data
     */
    public String getExtra() {
        return extra;
    }

    /**
     * Set some extra data linked to that person.
     *
     * @param extra the person's extra data
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
        if (!(o instanceof Person)) {
            return false;
        }
        Person person = (Person) o;
        return getId() == person.getId() && Objects.equals(getName(), person.getName())
                && Objects.equals(getEmailAddress(), person.getEmailAddress()) && Objects.equals(getCreditCard(),
                person.getCreditCard()) && Objects.equals(getCity(), person.getCity()) && Objects.equals(getState(),
                person.getState()) && Objects.equals(getEntryTime(), person.getEntryTime()) && Objects.equals(getExtra(),
                person.getExtra());
    }

    /**
     * Generated hashCode method, used to generate the hash values of objects.
     *
     * @return an integer whose value represents the hash value of the input object
     */
    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName(), getEmailAddress(), getCreditCard(),
                getCity(), getState(), getEntryTime(), getExtra());
    }

    /**
     * Generated toString method, used to print object's internal value.
     *
     * @return the string related to the object.
     */
    @Override
    public String toString() {
        return "Person{"
                + "id=" + id
                + ", name='" + name + '\''
                + ", emailAddress='" + emailAddress + '\''
                + ", creditCard='" + creditCard + '\''
                + ", city='" + city + '\''
                + ", state='" + state + '\''
                + ", entryTime=" + entryTime
                + ", extra='" + extra + '\''
                + '}';
    }
}
