/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * Boolean configuration option
 */
public class BooleanConfOption extends AbstractConfOption {
    /** Default value */
    private final boolean defaultValue;

    /**
     * Constructor
     *
     * @param key configuration key
     * @param defaultValue default value
     * @param description configuration description
     */
    public BooleanConfOption(String key, boolean defaultValue,
        String description) {
        super(key, description);
        this.defaultValue = defaultValue;
    }

    /**
     * Get the default value of this option
     *
     * @return default value
     */
    public boolean getDefaultValue() {
        return defaultValue;
    }

    @Override public boolean isDefaultValue(Configuration conf) {
        return get(conf) == defaultValue;
    }

    @Override public String getDefaultValueStr() {
        return Boolean.toString(defaultValue);
    }

    @Override public ConfOptionType getType() {
        return ConfOptionType.BOOLEAN;
    }

    /**
     * Lookup value in Configuration
     *
     * @param conf Configuration
     * @return value for key in conf, or defaultValue if not present
     */
    public boolean get(Configuration conf) {
        return conf.getBoolean(getKey(), defaultValue);
    }

    /**
     * Check if value is true
     *
     * @param conf Configuration
     * @return true if value is set and true, false otherwise
     */
    public boolean isFalse(Configuration conf) {
        return !get(conf);
    }

    /**
     * Check if value is false
     *
     * @param conf Configuration
     * @return true if value is set and true, false otherwise
     */
    public boolean isTrue(Configuration conf) {
        return get(conf);
    }

    /**
     * Set value in configuration for this key
     *
     * @param conf Configuration
     * @param value to set
     */
    public void set(Configuration conf, boolean value) {
        conf.setBoolean(getKey(), value);
    }

    /**
     * Set value in configuration if it hasn't been set already
     *
     * @param conf Configuration
     * @param value to set
     */
    public void setIfUnset(Configuration conf, boolean value) {
        conf.setBooleanIfUnset(getKey(), value);
    }
}
