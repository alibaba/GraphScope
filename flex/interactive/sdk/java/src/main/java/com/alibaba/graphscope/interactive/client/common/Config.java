/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client.common;

public class Config {
    private boolean enableTracing;
    private long readTimeout;
    private long writeTimeout;
    private long connectionTimeout;
    private int maxIdleConnections;
    private long keepAliveDuration;

    public static final long DEFAULT_READ_TIMEOUT = 5000000;
    public static final long DEFAULT_WRITE_TIMEOUT = 5000000;
    private static final long DEFAULT_CONNECTION_TIMEOUT = 5000000;
    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 128;
    private static final long DEFAULT_KEEP_ALIVE_DURATION = 5000;

    private Config() {
        this.enableTracing = false; // default not enable tracing.
        this.readTimeout = DEFAULT_READ_TIMEOUT;
        this.writeTimeout = DEFAULT_WRITE_TIMEOUT;
        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        this.maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTIONS;
        this.keepAliveDuration = DEFAULT_KEEP_ALIVE_DURATION;
    }

    public boolean isEnableTracing() {
        return enableTracing;
    }

    public long getReadTimeout() {
        return readTimeout;
    }

    public long getWriteTimeout() {
        return writeTimeout;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public long getKeepAliveDuration() {
        return keepAliveDuration;
    }

    public static class ConfigBuilder {
        Config config;

        public ConfigBuilder() {
            config = new Config();
        }

        public ConfigBuilder enableTracing(boolean value) {
            config.enableTracing = value;
            return this;
        }

        public ConfigBuilder readTimeout(long readTimeout) {
            config.readTimeout = readTimeout;
            return this;
        }

        public ConfigBuilder writeTimeout(long writeTimeout) {
            config.writeTimeout = writeTimeout;
            return this;
        }

        public ConfigBuilder connectionTimeout(long connectionTimeout) {
            config.connectionTimeout = connectionTimeout;
            return this;
        }

        public ConfigBuilder connectionPoolMaxIdle(int maxPoolIdle) {
            config.maxIdleConnections = maxPoolIdle;
            return this;
        }

        public ConfigBuilder keepAliveDuration(long keepAliveDuration) {
            config.keepAliveDuration = keepAliveDuration;
            return this;
        }

        public Config build() {
            return config;
        }
    }

    public static ConfigBuilder newBuilder() {
        return new ConfigBuilder();
    }
}
