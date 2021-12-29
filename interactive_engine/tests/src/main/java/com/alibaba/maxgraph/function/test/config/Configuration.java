/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.function.test.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Configuration {

    private final Properties properties;

    public Configuration() {
        this.properties = new Properties();
    }

    public Configuration(Properties properties) {
        this.properties = properties;
    }

    public Configuration(InputStream in) throws IOException {
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        properties = new Properties();
        properties.load(reader);
    }

    /**
     * Load a configuration file.
     * The properties of this file will override properties of previously added files.
     *
     * @param file file-path in the local filesystem of the configuration to be added.
     */
    public void load(String file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file)) {
            properties.load(inputStream);
        }
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
     * Get the value of the {@code name} property.
     * If no such property exists, then {@code defaultValue} is returned.
     *
     * @param name         property name.
     * @param defaultValue default value.
     * @return property value, or defaultValue if the property doesn't exist.
     */
    public String get(String name, String defaultValue) {
        return this.properties.getProperty(name, defaultValue);
    }

    public Properties getProperties() {
        return this.properties;
    }
}
