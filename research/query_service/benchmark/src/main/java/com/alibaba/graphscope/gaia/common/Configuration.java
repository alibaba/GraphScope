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
package com.alibaba.graphscope.gaia.common;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

public class Configuration {
    protected HashMap<String, String> settings = new HashMap<>();

    public Configuration(Properties properties) {
        for (Object key : properties.keySet()) {
            settings.put((String) key, properties.getProperty((String) key));
        }
    }

    public static final String GREMLIN_SERVER_ENDPOINT = "endpoint";
    public static final String GREMLIN_USERNAME = "username";
    public static final String GREMLIN_PASSWORD = "password";

    public static final String THREAD_COUNT = "thread_count";
    public static final String QUERY_DIR = "queryDir";
    public static final String PARAMETER_DIR = "interactive.parameters.dir";
    public static final String WARMUP_EVERY_QUERY = "warmup.every.query";
    public static final String OPERATION_COUNT_EVERY_QUERY = "operation.count.every.query";
    public static final String PRINT_QUERY_NAME = "printQueryNames";
    public static final String PRINT_QUERY_RESULT = "printQueryResults";

    public Optional<String> getOption(String key) {
        String value = settings.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    public String getString(String key) {
        Optional<String> optional = getOption(key);
        if (optional.isPresent()) {
            return optional.get();
        } else {
            throw new IllegalArgumentException("Required key not found: " + key);
        }
    }

    public String getString(String key, String defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.orElse(defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Integer::parseInt).orElse(defaultValue);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Boolean::parseBoolean).orElse(defaultValue);
    }
}
