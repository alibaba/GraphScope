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
package com.alibaba.maxgraph.common.cluster;

import com.alibaba.maxgraph.sdkcommon.util.PropertyUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaxGraphConfiguration for MaxGraph
 *
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-05-08 上午11:19
 **/

public class MaxGraphConfiguration {

    public static final Logger LOG = LoggerFactory.getLogger(MaxGraphConfiguration.class);
    protected final ConcurrentHashMap<String, String> settings = new ConcurrentHashMap<String, String>();

    // common config
    public static final String CLUSTER_HADOOP_HOME = "executor.download.data.hadoop.home";

    public MaxGraphConfiguration() {
    }

    public MaxGraphConfiguration(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Preconditions.checkNotNull(entry.getKey());
            if (entry.getValue() != null) {
                settings.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public MaxGraphConfiguration(Properties props) {
        for (Object key : props.keySet()) {
            settings.put((String) key, props.getProperty((String) key));
        }
    }

    public MaxGraphConfiguration set(String key, Object value) {
        Preconditions.checkArgument(key != null, "null key!");
        Preconditions.checkArgument(value != null, "null toInt for " + key + "!");
        settings.put(key, value.toString());
        return this;
    }

    public MaxGraphConfiguration setAll(Map<String, Object> settings) {
        settings.forEach(this::set);
        return this;
    }

    public ConcurrentHashMap<String, String> getAll() {
        return settings;
    }

    public void putAll(MaxGraphConfiguration other) {
        if (other != null) {
            settings.putAll(other.getAll());
        }
    }

    public void putAll(Map<String, String> conf) {
        if (conf != null) {
            settings.putAll(conf);
        }
    }

    /**
     * Get a parameter as an Optional.
     */
    public Optional<String> getOption(String key) {
        String value = settings.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    /**
     * Get a parameter, falling back to a default if not set
     */
    public String get(String key, String defaultValue) {
        return getString(key, defaultValue);
    }

    /**
     * Get a parameter as a String, falling back to a default if not set
     */
    public String getString(String key, String defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.orElse(defaultValue);
    }

    /**
     * Get a parameter as a String, throw IllegalArgumentException if not found
     */
    public String getString(String key) {
        Optional<String> optional = getOption(key);
        if (optional.isPresent()) {
            return optional.get();
        } else {
            throw new IllegalArgumentException("Required key not found: " + key);
        }
    }

    public long getLong(String key) {
        Optional<String> optional = getOption(key);
        if (optional.isPresent()) {
            return Long.parseLong(optional.get());
        } else {
            throw new IllegalArgumentException("Required key not found: " + key);
        }
    }

    /**
     * Get a parameter as a long, falling back to a default if not set
     */
    public long getLong(String key, long defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Long::parseLong).orElse(defaultValue);
    }

    /**
     * Get a parameter as a double, falling back to a default if not set
     */
    public double getDouble(String key, double defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Double::parseDouble).orElse(defaultValue);
    }

    /**
     * Get a parameter as an int, falling back to a default if not set
     */
    public int getInt(String key, int defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Integer::parseInt).orElse(defaultValue);
    }

    /**
     * Get a parameter as an int, throw IllegalArgumentException if not found
     */
    public int getInt(String key) {
        Optional<String> optional = getOption(key);
        if (optional.isPresent()) {
            return Integer.parseInt(optional.get());
        } else {
            throw new IllegalArgumentException("Required key not found: " + key);
        }
    }

    /**
     * Get a parameter as a boolean, falling back to a default if not set
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Optional<String> optional = getOption(key);
        return optional.map(Boolean::parseBoolean).orElse(defaultValue);
    }

    @VisibleForTesting
    void clear() {
        this.settings.clear();
    }


    public Map<String, String> getProperties() {
        return ImmutableMap.copyOf(settings);
    }

    /**
     * load the the galaxy conf from properties file
     */
    public static MaxGraphConfiguration loadConf(String fileName, boolean resourcePath) throws IOException {
        Properties props = PropertyUtil.getProperties(fileName, resourcePath);
        return new MaxGraphConfiguration(props);
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    @Override
    public String toString() {
        return settings.toString();
    }

    /**
     * get server configs in MaxGraphConfiguration
     *
     * @return
     */
    public static List<String> getCommonConfigs() {
        return getAllStaticMembers(MaxGraphConfiguration.class);
    }

    public String getClusterHadoopHome() {
        return getString(CLUSTER_HADOOP_HOME);
    }

    /**
     * get all configs by static members
     *
     * @param configClass
     * @return
     */
    public static List<String> getAllStaticMembers(Class configClass) {
        Field[] fields = null;
        List<String> keys = Lists.newArrayList();
        try {
            fields = Class.forName(configClass.getCanonicalName()).getFields();
            for (Field field : fields) {
                if ("String".equals(field.getType().getSimpleName()) && Modifier.isStatic(field.getModifiers())) {
                    keys.add(field.get(null).toString());
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        }

        return keys;
    }
}
