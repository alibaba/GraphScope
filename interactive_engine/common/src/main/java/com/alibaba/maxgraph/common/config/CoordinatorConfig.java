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

public class CoordinatorConfig {
    public static final Config<Long> SNAPSHOT_INCREASE_INTERVAL_MS =
            Config.longConfig("snapshot.increase.interval.ms", 1000L);

    public static final Config<Long> OFFSETS_PERSIST_INTERVAL_MS =
            Config.longConfig("offsets.persist.interval.ms", 3000L);

    public static final Config<Boolean> LOG_RECYCLE_ENABLE =
            Config.boolConfig("log.recycle.enable", true);

    public static final Config<Long> LOG_RECYCLE_INTERVAL_SECOND =
            Config.longConfig("log.recycle.interval.second", 60L);

    public static final Config<String> FILE_META_STORE_PATH =
            Config.stringConfig("file.meta.store.path", "./meta");
}
