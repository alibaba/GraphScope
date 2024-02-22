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
            Config.boolConfig("neo4j.bolt.server.disabled", true);

    public static final Config<Integer> NEO4J_BOLT_SERVER_PORT =
            Config.intConfig("neo4j.bolt.server.port", 7687);

    public static final Config<Integer> QUERY_EXECUTION_TIMEOUT_MS =
            Config.intConfig("query.execution.timeout.ms", 3000000);

    public static final Config<String> ENGINE_TYPE = Config.stringConfig("engine.type", "pegasus");

    public static final Config<Integer> FRONTEND_SERVER_ID =
            Config.intConfig("frontend.server.id", 0);

    public static final Config<Integer> FRONTEND_SERVER_NUM =
            Config.intConfig("frontend.server.num", 1);

    public static final Config<String> CALCITE_DEFAULT_CHARSET =
            Config.stringConfig("calcite.default.charset", "UTF-8");

    public static final Config<Integer> QUERY_CACHE_SIZE =
            Config.intConfig("query.cache.size", 100);

    public static final Config<Integer> QUERY_PER_SECOND_LIMIT =
            Config.intConfig("frontend.query.per.second.limit", 2147483647);

    public static final Config<Boolean> GRAPH_TYPE_INFERENCE_ENABLED =
            Config.boolConfig("graph.type.inference.enabled", true);

    public static final Config<String> GREMLIN_SCRIPT_LANGUAGE_NAME =
            Config.stringConfig("gremlin.script.language.name", "antlr_gremlin_traversal");
}
