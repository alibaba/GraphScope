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

    public static final Config<Long> LOG_RECYCLE_INTERVAL_SECOND =
            Config.longConfig("log.recycle.interval.second", 60L);

    public static final Config<String> FILE_META_STORE_PATH =
            Config.stringConfig("file.meta.store.path", "./meta");

    public static final Config<Integer> BACKUP_CREATION_BUFFER_MAX_COUNT =
            Config.intConfig("backup.creation.buffer.max.count", 16);

    public static final Config<Integer> BACKUP_GC_INTERVAL_HOURS =
            Config.intConfig("backup.gc.interval.hours", 12);

    public static final Config<Boolean> BACKUP_AUTO_SUBMIT =
            Config.boolConfig("backup.auto.submit", false);

    public static final Config<Integer> BACKUP_AUTO_SUBMIT_INTERVAL_HOURS =
            Config.intConfig("backup.auto.submit.interval.hours", 24);
}
