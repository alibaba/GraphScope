package com.alibaba.maxgraph.v2.common.config;

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
