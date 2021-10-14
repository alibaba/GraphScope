/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.config;

import com.alibaba.maxgraph.common.RoleType;

public class CommonConfig {
    public static final String NODE_COUNT_FORMAT = "%s.node.count";

    public static final Config<String> ROLE_NAME = Config.stringConfig("role.name", "");

    public static final Config<Integer> NODE_IDX = Config.intConfig("node.idx", 0);

    public static final Config<String> RPC_HOST = Config.stringConfig("rpc.host", "");

    public static final Config<Integer> RPC_PORT = Config.intConfig("rpc.port", 0);

    public static final Config<Integer> RPC_THREAD_COUNT =
            Config.intConfig(
                    "rpc.thread.count",
                    Math.min(Runtime.getRuntime().availableProcessors() * 2, 64));

    public static final Config<Integer> RPC_MAX_BYTES_MB = Config.intConfig("rpc.max.bytes.mb", 4);

    public static final Config<Integer> STORE_NODE_COUNT =
            Config.intConfig(String.format(NODE_COUNT_FORMAT, RoleType.STORE.getName()), 1);

    public static final Config<Integer> FRONTEND_NODE_COUNT =
            Config.intConfig(String.format(NODE_COUNT_FORMAT, RoleType.FRONTEND.getName()), 1);

    public static final Config<Integer> INGESTOR_NODE_COUNT =
            Config.intConfig(String.format(NODE_COUNT_FORMAT, RoleType.INGESTOR.getName()), 2);

    public static final Config<Integer> COORDINATOR_NODE_COUNT =
            Config.intConfig(String.format(NODE_COUNT_FORMAT, RoleType.COORDINATOR.getName()), 1);

    public static final Config<Integer> INGESTOR_QUEUE_COUNT =
            Config.intConfig("ingestor.queue.count", 2);

    public static final Config<Integer> PARTITION_COUNT = Config.intConfig("partition.count", 1);

    public static final Config<String> GRAPH_NAME = Config.stringConfig("graph.name", "maxgraph");

    public static final Config<Long> METRIC_UPDATE_INTERVAL_MS =
            Config.longConfig("metric.update.interval.ms", 5000L);

    /**
     * Get the engine type
     *
     * @return The engine type
     */
    public static final Config<String> ENGINE_TYPE = Config.stringConfig("engine.type", "maxgraph");

    public static final Config<String> LOG4RS_CONFIG = Config.stringConfig("log4rs.config", "");

    public static final Config<String> DISCOVERY_MODE =
            Config.stringConfig("discovery.mode", "file"); // others: zookeeper

    public static final Config<Integer> ID_ALLOCATE_SIZE =
            Config.intConfig("id.allocate.size", 1000000);
}
