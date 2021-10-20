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

public class ZkConfig {
    public static final Config<String> ZK_BASE_PATH =
            Config.stringConfig("zk.base.path", "/maxgraph/default_graph");

    public static final Config<String> ZK_CONNECT_STRING =
            Config.stringConfig("zk.connect.string", "localhost:2181/default_graph");

    public static final Config<Integer> ZK_CONNECTION_TIMEOUT_MS =
            Config.intConfig("zk.connection.timeout.ms", 1000);

    public static final Config<Integer> ZK_SESSION_TIMEOUT_MS =
            Config.intConfig("zk.session.timeout.ms", 10000);

    public static final Config<Integer> ZK_BASE_SLEEP_MS =
            Config.intConfig("zk.base.sleep.ms", 1000);

    public static final Config<Integer> ZK_MAX_SLEEP_MS =
            Config.intConfig("zk.max.sleep.ms", 45000);

    public static final Config<Integer> ZK_MAX_RETRY =
            Config.intConfig("zk.max.retry", 29);

    public static final Config<Boolean> ZK_AUTH_ENABLE =
            Config.boolConfig("zk.auth.enable", false);

    public static final Config<String> ZK_AUTH_USER =
            Config.stringConfig("zk.auth.user", "");

    public static final Config<String> ZK_AUTH_PASSWORD =
            Config.stringConfig("zk.auth.password", "");

    public static final Config<String> ZK_ENDPOINT_NODE =
            Config.stringConfig("zk.endpoint.node", "/endpoint");
}
