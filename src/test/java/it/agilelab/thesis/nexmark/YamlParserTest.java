package it.agilelab.thesis.nexmark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class YamlParserTest {
    private static YamlParser parser;

    @BeforeAll
    public static void setUp() {
        parser = new YamlParser();
    }

    @Test
    public void testParse() throws IOException {
        String yamlFilePath = "config/nexmark-conf.yaml";
        assertNotNull(parser.parse(yamlFilePath));
        assertTrue(parser.getValue("nexmark.number.events") instanceof Long || parser.getValue("nexmark.number.events") instanceof Integer);
    }

}
