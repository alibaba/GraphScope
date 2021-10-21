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

    public static final Config<Integer> EXECUTOR_GRAPH_PORT =
            Config.intConfig("executor.graph.port", 0);

    public static final Config<Integer> EXECUTOR_QUERY_PORT =
            Config.intConfig("executor.query.port", 0);

    public static final Config<Integer> EXECUTOR_ENGINE_PORT =
            Config.intConfig("executor.engine.port", 0);

}
