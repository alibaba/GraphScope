/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.config;

public class QueryTimeoutConfig {
    // timeout in milliseconds for engine execution
    private final long engineTimeoutMS;
    // timeout in milliseconds for channel communication
    private final long channelTimeoutMS;
    // timeout in milliseconds for total query execution
    private final long executionTimeoutMS;
    private static final double GRADUAL_FACTOR = 0.1d;

    public QueryTimeoutConfig(long engineTimeoutMS) {
        this.engineTimeoutMS = engineTimeoutMS;
        this.channelTimeoutMS = (long) (engineTimeoutMS * (1 + GRADUAL_FACTOR));
        this.executionTimeoutMS = (long) (engineTimeoutMS * (1 + 2 * GRADUAL_FACTOR));
    }

    public long getExecutionTimeoutMS() {
        return executionTimeoutMS;
    }

    public long getChannelTimeoutMS() {
        return channelTimeoutMS;
    }

    public long getEngineTimeoutMS() {
        return engineTimeoutMS;
    }

    @Override
    public String toString() {
        return "QueryTimeoutConfig{"
                + "executionTimeoutMS="
                + executionTimeoutMS
                + ", channelTimeoutMS="
                + channelTimeoutMS
                + ", engineTimeoutMS="
                + engineTimeoutMS
                + '}';
    }
}
