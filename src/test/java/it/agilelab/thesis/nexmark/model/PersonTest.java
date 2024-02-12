package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PersonTest {

    private Person person;

    @BeforeEach
    public void setup() {
        long id = 1;
        String name = "John Doe";
        String emailAddress = "john.doe@example.com";
        String creditCard = "1234567890";
        String city = "New York";
        String state = "NY";
        Instant entryTime = Instant.now();
        String extra = "Some extra data";

        person = new Person(id, name, emailAddress, creditCard, city, state, entryTime, extra);
    }

    @Test
    void testGetId() {
        long expectedId = 1;
        assertEquals(expectedId, person.getId());
    }

    @Test
    void testGetName() {
        String expectedName = "John Doe";
        assertEquals(expectedName, person.getName());
    }

    @Test
    void testGetEmailAddress() {
        String expectedEmailAddress = "john.doe@example.com";
        assertEquals(expectedEmailAddress, person.getEmailAddress());
    }

    @Test
    void testGetCreditCard() {
        String expectedCreditCard = "1234567890";
        assertEquals(expectedCreditCard, person.getCreditCard());
    }

    @Test
    void testGetCity() {
        String expectedCity = "New York";
        assertEquals(expectedCity, person.getCity());
    }

    @Test
    void testGetState() {
        String expectedState = "NY";
        assertEquals(expectedState, person.getState());
    }

    @Test
    void testGetEntryTime() {
        Instant expectedEntryTime = Instant.now();
        // This kind of test needs a delta, because the two instants are not exactly the same
        // since they are created at the same time, but not exactly at the same time
        assertEquals(expectedEntryTime.toEpochMilli(), person.getEntryTime().toEpochMilli(), 100);
    }

    @Test
    void testGetExtra() {
        String expectedExtra = "Some extra data";
        assertEquals(expectedExtra, person.getExtra());
    }
}
