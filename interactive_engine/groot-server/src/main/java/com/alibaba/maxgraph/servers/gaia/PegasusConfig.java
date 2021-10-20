package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.maxgraph.common.config.Config;

public class PegasusConfig {
    public static final Config<Integer> PEGASUS_WORKER_NUM =
            Config.intConfig("pegasus.worker.num", 2);

    public static final Config<Integer> PEGASUS_TIMEOUT =
            Config.intConfig("pegasus.timeout", 240000);

    public static final Config<Integer> PEGASUS_BATCH_SIZE =
            Config.intConfig("pegasus.batch.size", 1024);

    public static final Config<Integer> PEGASUS_OUTPUT_CAPACITY =
            Config.intConfig("pegasus.output.capacity", 16);

    public static final Config<Integer> PEGASUS_MEMORY_LIMIT =
            Config.intConfig("pegasus.memory.limit", Integer.MAX_VALUE);
}
