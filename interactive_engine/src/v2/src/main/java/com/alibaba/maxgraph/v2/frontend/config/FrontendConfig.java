package com.alibaba.maxgraph.v2.frontend.config;

import com.alibaba.maxgraph.v2.common.config.Config;

/**
 * Frontend related config
 */
public class FrontendConfig {
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

    /**
     * Get the graph store type for current system
     *
     * @return The store type, memory for default
     */
    public static final Config<String> GRAPH_STORE_TYPE =
            Config.stringConfig("graph.store.type", "memory");

    /**
     * The batch size of result written from server to client
     */
    public static final Config<Integer> RESULT_ITERATION_BATCH_SIZE =
            Config.intConfig("gremlin.result.iteration.batch.size", 64);

    public static final Config<Integer> QUERY_RESPONSE_BUFFER_QUEUE_SIZE =
            Config.intConfig("query.response.buffer.queue.size", 64);

    public static final Config<String> QUERY_VINEYARD_SCHEMA_PATH =
            Config.stringConfig("query.vineyard.schema.path", "");

    public static final Config<Boolean> GREMLIN_SERVER_VERTEX_CACHE_ENABLE =
            Config.boolConfig("gremlin.server.vertex.cache.enable", true);

    public static final Config<Boolean> TIMELY_FETCH_PROP_FLAG =
            Config.boolConfig("timely.fetch.prop.flag", true);

    public static final Config<Integer> TIMELY_RESULT_ITERATION_BATCH_SIZE =
            Config.intConfig("timely.result.iteration.batch.size", 64);

    public static final Config<Boolean> TIMELY_GLOBAL_PULL_GRAPH_FLAG =
            Config.boolConfig("timely.global.pull.graph.flag", false);
}
