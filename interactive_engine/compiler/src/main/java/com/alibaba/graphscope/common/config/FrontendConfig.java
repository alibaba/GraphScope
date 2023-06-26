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

public class FrontendConfig {
    public static final Config<Boolean> GREMLIN_SERVER_DISABLED =
            Config.boolConfig("gremlin.server.disabled", false);

    public static final Config<Integer> GREMLIN_SERVER_PORT =
            Config.intConfig("gremlin.server.port", 8182);

    public static final Config<Boolean> NEO4J_BOLT_SERVER_DISABLED =
            Config.boolConfig("neo4j.bolt.server.disabled", false);

    public static final Config<Integer> NEO4J_BOLT_SERVER_PORT =
            Config.intConfig("neo4j.bolt.server.port", 7687);

    public static final Config<String> ENGINE_TYPE = Config.stringConfig("engine.type", "pegasus");
}
