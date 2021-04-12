package com.alibaba.maxgraph.v2.common.config;

import com.alibaba.maxgraph.proto.v2.ConfigPb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public class Configs {

    private Properties properties;

    public Configs() {
        this.properties = new Properties();
    }

    public Configs(String configFile) throws IOException {
        this();
        try (InputStream is = new FileInputStream(configFile)) {
            this.properties.load(is);
        }
    }

    public Configs(Properties properties) {
        this.properties = properties;
    }

    /**
     * Get the value of the {@code name} property, {@code null} if no such property exists.
     *
     * @param name the property name.
     * @return property value, or null if no such property exists.
     */
    public String get(String name) {
        return this.properties.getProperty(name);
    }

    /**
     * Get the value of the {@code name} property. If no such property exists, then {@code defaultValue} is returned.
     *
     * @param name         property name.
     * @param defaultValue default value.
     * @return property value, or defaultValue if the property doesn't exist.
     */
    public String get(String name, String defaultValue) {
        return this.properties.getProperty(name, defaultValue);
    }

    public ConfigPb toProto() {
        ConfigPb.Builder builder = ConfigPb.newBuilder();
        for (Map.Entry<Object, Object> entry : this.properties.entrySet()) {
            builder.putConfigs(entry.getKey().toString(), entry.getValue().toString());
        }
        return builder.build();
    }

    public Properties getInnerProperties() {
        return this.properties;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Configs fromConfigs) {
        return new Builder(fromConfigs);
    }

    public static Builder newBuilder(Properties properties) {
        return new Builder(properties);
    }

    public static class Builder {

        private Properties properties;

        private Builder() {
            this.properties = new Properties();
        }

        private Builder(Configs fromConfigs) {
            this.properties = new Properties();
            fromConfigs.properties.entrySet().forEach(e -> this.properties.put(e.getKey(), e.getValue()));
        }

        private Builder(Properties properties) {
            this();
            this.properties.putAll(properties);
        }

        public Builder put(String key, String val) {
            this.properties.put(key, val);
            return this;
        }

        public Configs build() {
            return new Configs(this.properties);
        }
    }
}
