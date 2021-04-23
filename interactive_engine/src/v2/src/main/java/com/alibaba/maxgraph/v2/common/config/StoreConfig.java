package com.alibaba.maxgraph.v2.common.config;

public class StoreConfig {
    public static final Config<String> STORE_DATA_PATH =
            Config.stringConfig("store.data.path", "/maxgraph_data");

    public static final Config<Integer> STORE_WRITE_THREAD_COUNT =
            Config.intConfig("store.write.thread.count", 1);

    public static final Config<Integer> STORE_QUEUE_BUFFER_SIZE =
            Config.intConfig("store.queue.buffer.size", 128);

    public static final Config<Long> STORE_QUEUE_WAIT_MS =
            Config.longConfig("store.queue.wait.ms", 3000L);

    public static final Config<Long> STORE_COMMIT_INTERVAL_MS =
            Config.longConfig("store.commit.interval.ms", 1000L);

}
