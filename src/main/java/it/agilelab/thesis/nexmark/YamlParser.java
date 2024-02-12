package it.agilelab.thesis.nexmark;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class YamlParser {
    private final Map<String, Object> data;

    public YamlParser() {
        this.data = new HashMap<>();
    }

    /**
     * Parse a yaml file and return a map of key-value pairs.
     *
     * @param yamlFilePath the path of the yaml file
     * @return a map of key-value pairs
     * @throws IOException if the file is not found
     */
    public Map<String, Object> parse(final String yamlFilePath) throws IOException {
        InputStream inputStream = new FileInputStream(yamlFilePath);
        Yaml yaml = new Yaml();
        this.data.putAll(yaml.load(inputStream));
        inputStream.close();
        return this.data;
    }

    /**
     * Get the value of a key from the map.
     *
     * @param key the key
     * @param <T> the type of the value
     * @return the value of the key
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(final String key) {
        return (T) this.data.get(key);
    }

    /**
     * Get the map of key-value pairs.
     *
     * @return the map of key-value pairs
     */
    public Map<String, Object> getData() {
        return this.data;
    }
}
