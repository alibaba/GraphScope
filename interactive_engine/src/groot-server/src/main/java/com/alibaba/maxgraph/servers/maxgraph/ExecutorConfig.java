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
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.maxgraph.common.config.Config;

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
