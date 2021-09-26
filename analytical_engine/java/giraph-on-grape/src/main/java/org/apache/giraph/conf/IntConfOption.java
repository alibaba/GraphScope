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
 * Integer configuration option
 */
public class IntConfOption extends AbstractConfOption {

    /**
     * Default value
     */
    private final int defaultValue;

    /**
     * Constructor
     *
     * @param key          key
     * @param defaultValue default value
     * @param description  configuration description
     */
    public IntConfOption(String key, int defaultValue, String description) {
        super(key, description);
        this.defaultValue = defaultValue;
    }

    /**
     * Constructor
     *
     * @param key          key
     * @param defaultValue default value
     * @param description  configuration description
     */
    public IntConfOption(String key, long defaultValue, String description) {
        super(key, description);
        this.defaultValue = (int) defaultValue;
    }

    public int getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isDefaultValue(Configuration conf) {
        return get(conf) == defaultValue;
    }

    @Override
    public String getDefaultValueStr() {
        return Integer.toString(defaultValue);
    }

    @Override
    public ConfOptionType getType() {
        return ConfOptionType.INTEGER;
    }

    /**
     * Lookup value
     *
     * @param conf Configuration
     * @return value for key, or default value if not set
     */
    public int get(Configuration conf) {
        return conf.getInt(getKey(), defaultValue);
    }

    /**
     * Set value
     *
     * @param conf  Configuration
     * @param value to set
     */
    public void set(Configuration conf, int value) {
        conf.setInt(getKey(), value);
    }

    /**
     * Set value if it's not already present
     *
     * @param conf  Configuration
     * @param value to set
     */
    public void setIfUnset(Configuration conf, int value) {
        if (!contains(conf)) {
            conf.setInt(getKey(), value);
        }
    }
}
