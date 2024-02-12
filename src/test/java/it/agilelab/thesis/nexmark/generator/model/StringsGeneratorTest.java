package it.agilelab.thesis.nexmark.generator.model;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class StringsGeneratorTest {

    @Test
    void testCreateRandomStringWithNegativeLength() {
        Random random = new Random();
        assertThrows(IllegalArgumentException.class, () -> StringsGenerator.nextString(random, -1));
    }

    @Test
    void testCreateRandomStringWithZeroLength() {
        Random random = new Random();
        assertThrows(IllegalArgumentException.class, () -> StringsGenerator.nextString(random, 0));
    }

    @Test
    void testCreateRandomStringWithPositiveLength() {
        Random random = new Random();
        String string = StringsGenerator.nextString(random, 5);
        LoggerFactory.getLogger(StringsGeneratorTest.class).info(() -> "The string created is: " + string);
        assertTrue(string.length() <= 5);
    }

    @Test
    void testCreateRandomStringWithSpecialCharacter() {
        Random random = new Random();
        String string = StringsGenerator.nextString(random, 5, 'a');
        LoggerFactory.getLogger(StringsGeneratorTest.class).info(() -> "The string created is: " + string);
        assertTrue(string.length() <= 5);
    }

    @Test
    void testCreateRandomStringWithFixedLength() {
        Random random = new Random();
        String string = StringsGenerator.nextExactString(random, 5);
        LoggerFactory.getLogger(StringsGeneratorTest.class).info(() -> "The string created is: " + string);
        assertEquals(5, string.length());
    }

    @Test
    void testNextExtra() {
        Random random = new Random();
        int currentLength = 5;
        int desiredAverageLength = 20;
        String string = StringsGenerator.nextExtra(random, currentLength, desiredAverageLength);
        LoggerFactory.getLogger(StringsGeneratorTest.class).info(() -> "The string created is: " + string);
        assertTrue(string.length() <= 20);
    }

}
