package com.alibaba.maxgraph.v2.common.config;

import com.alibaba.maxgraph.v2.common.discovery.RoleType;

public class DiscoveryConfig {
    public static final String DNS_NAME_PREFIX_FORMAT = "dns.name.prefix.%s";

    public static final Config<String> DNS_NAME_PREFIX_FRONTEND =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.FRONTEND.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_INGESTOR =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.INGESTOR.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_COORDINATOR =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.COORDINATOR.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_STORE =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.STORE.getName()), "");
}
