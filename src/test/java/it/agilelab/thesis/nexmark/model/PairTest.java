package it.agilelab.thesis.nexmark.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PairTest {

    @Test
    void createPairTest() {
        Pair<String, String> pair = new Pair<>("first", "second");
        assertNotNull(pair);
    }

    @Test
    void getFirstTest() {
        Pair<String, String> pair = new Pair<>("first", "second");
        assertNotNull(pair.getFirst());
        assertEquals("first", pair.getFirst());
    }

    @Test
    void getSecondTest() {
        Pair<String, String> pair = new Pair<>("first", "second");
        assertNotNull(pair.getSecond());
        assertEquals("second", pair.getSecond());
    }
}
