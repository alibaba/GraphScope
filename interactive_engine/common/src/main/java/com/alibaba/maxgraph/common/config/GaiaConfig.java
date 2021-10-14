package com.alibaba.maxgraph.common.config;

public class GaiaConfig {
    public static final Config<Boolean> GAIA_ENABLE =
            Config.boolConfig("gaia.enable", false);

    public static final Config<Boolean> GAIA_REPORT =
            Config.boolConfig("gaia.report", false);

    public static final Config<Integer> GAIA_RPC_PORT =
            Config.intConfig("gaia.rpc.port", 0);

    public static final Config<Integer> GAIA_ENGINE_PORT =
            Config.intConfig("gaia.engine.port", 0);

    public static final Config<Boolean> GAIA_NONBLOCKING =
            Config.boolConfig("gaia.nonblocking", false);

    public static final Config<Integer> GAIA_READ_TIMEOUT_MS =
            Config.intConfig("gaia.read.timeout.ms", 0);

    public static final Config<Integer> GAIA_WRITE_TIMEOUT_MS =
            Config.intConfig("gaia.write.timeout.ms", 0);

    public static final Config<Integer> GAIA_READ_SLAB_SIZE =
            Config.intConfig("gaia.read.slab.size", 0);

    public static final Config<Boolean> GAIA_NO_DELAY =
            Config.boolConfig("gaia.no.delay", false);

    public static final Config<Integer> GAIA_SEND_BUFFER =
            Config.intConfig("gaia.send.buffer", 0);

    public static final Config<Integer> GAIA_HEARTBEAT_SEC =
            Config.intConfig("gaia.heartbeat.sec", 0);

    public static final Config<Integer> GAIA_MAX_POOL_SIZE =
            Config.intConfig("gaia.max.pool.size", 0);
}
