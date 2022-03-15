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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * String Configuration option
 */
public class StrConfOption extends AbstractConfOption {

    /**
     * Default value
     */
    private final String defaultValue;

    /**
     * Constructor
     *
     * @param key          key
     * @param defaultValue default value
     * @param description  configuration description
     */
    public StrConfOption(String key, String defaultValue, String description) {
        super(key, description);
        this.defaultValue = defaultValue;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isDefaultValue(Configuration conf) {
        return Objects.equal(get(conf), defaultValue);
    }

    @Override
    public String getDefaultValueStr() {
        return defaultValue;
    }

    @Override
    public ConfOptionType getType() {
        return ConfOptionType.STRING;
    }

    /**
     * Lookup value
     *
     * @param conf Configuration
     * @return value for key, or defaultValue
     */
    public String get(Configuration conf) {
        return conf.get(getKey(), defaultValue);
    }

    /**
     * Lookup value with user defined defaultValue
     *
     * @param conf       Configuration
     * @param defaultVal default value to use
     * @return value for key, or defaultVal passed in
     */
    public String getWithDefault(Configuration conf, String defaultVal) {
        return conf.get(getKey(), defaultVal);
    }

    /**
     * Get array of values for key
     *
     * @param conf Configuration
     * @return array of values for key
     */
    public String[] getArray(Configuration conf) {
        return conf.getStrings(getKey(), defaultValue);
    }

    /**
     * Get list of values for key
     *
     * @param conf Configuration
     * @return list of values for key
     */
    public List<String> getList(Configuration conf) {
        return Lists.newArrayList(getArray(conf));
    }

    /**
     * Set value for key
     *
     * @param conf  Configuration
     * @param value to set
     */
    public void set(Configuration conf, String value) {
        conf.set(getKey(), value);
    }

    /**
     * Set value if not already present
     *
     * @param conf  Configuration
     * @param value to set
     */
    public void setIfUnset(Configuration conf, String value) {
        conf.setIfUnset(getKey(), value);
    }
}
