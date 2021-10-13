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
package com.alibaba.maxgraph.common.config;

/**
 * Gremlin related config
 */
public class GremlinConfig {
    /**
     * Get gremlin server port
     *
     * @return The gremlin server port
     */
    public static final Config<Integer> GREMLIN_PORT =
            Config.intConfig("gremlin.server.port", 0);

    /**
     * Get gremlin server write buffer high water
     *
     * @return The gremlin server netty write buffer high water
     */
    public static final Config<Integer> SERVER_WRITE_BUFFER_HIGH_WATER =
            Config.intConfig("gremlin.server.buffer.high.water", 16 * 1024 * 1024);

    /**
     * Get gremlin server write buffer low water
     *
     * @return The gremlin server netty write buffer low water
     */
    public static final Config<Integer> SERVER_WRITE_BUFFER_LOW_WATER =
            Config.intConfig("gremlin.server.buffer.low.water", 8 * 1024 * 1024);

}
