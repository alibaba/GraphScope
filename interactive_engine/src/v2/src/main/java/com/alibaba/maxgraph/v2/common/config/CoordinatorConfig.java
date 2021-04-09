package com.alibaba.maxgraph.v2.common.config;

public class CoordinatorConfig {
    public static final Config<Long> SNAPSHOT_INCREASE_INTERVAL_MS =
            Config.longConfig("snapshot.increase.interval.ms", 1000L);

    public static final Config<Long> OFFSETS_PERSIST_INTERVAL_MS =
            Config.longConfig("offsets.persist.interval.ms", 3000L);

    public static final Config<Long> LOG_RECYCLE_INTERVAL_SECOND =
            Config.longConfig("log.recycle.interval.second", 60L);
}
