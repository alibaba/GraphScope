package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.v2.common.config.Config;

public class ExecutorConfig {
    public static final Config<Long> EXECUTOR_HB_INTERVAL_MS =
            Config.longConfig("executor.hb.interval.ms", 1000);
    public static final Config<Integer> EXECUTOR_WORKER_PER_PROCESS =
            Config.intConfig("executor.worker.per.process", 1);
    public static final Config<String> EXECUTOR_INNER_CPU_CONFIG =
            Config.stringConfig("executor.inner.cpu.config", "");
    public static final Config<Integer> EXECUTOR_QUERY_QUEUE_SIZE =
            Config.intConfig("executor.query.queue.size", 1024);
    public static final Config<Integer> EXECUTOR_QUERY_THREAD_COUNT =
            Config.intConfig("executor.query.thread.count", 1);
    public static final Config<Integer> EXECUTOR_QUERY_MANAGER_THREAD_COUNT =
            Config.intConfig("executor.query.manager.thread.count", 1);
    public static final Config<Integer> EXECUTOR_QUERY_STORE_THREAD_COUNT =
            Config.intConfig("executor.query.store.thread.count", 1);
}
