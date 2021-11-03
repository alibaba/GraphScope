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
package com.alibaba.maxgraph.common.config;

public class BackupConfig {
    public static final Config<Boolean> BACKUP_ENABLE =
            Config.boolConfig("backup.enable", false);

    public static final Config<Integer> BACKUP_CREATION_BUFFER_MAX_COUNT =
            Config.intConfig("backup.creation.buffer.max.count", 16);

    public static final Config<Integer> BACKUP_GC_INTERVAL_HOURS =
            Config.intConfig("backup.gc.interval.hours", 12);

    public static final Config<Boolean> BACKUP_AUTO_SUBMIT =
            Config.boolConfig("backup.auto.submit", false);

    public static final Config<Integer> BACKUP_AUTO_SUBMIT_INTERVAL_HOURS =
            Config.intConfig("backup.auto.submit.interval.hours", 24);

    public static final Config<Integer> STORE_BACKUP_THREAD_COUNT =
            Config.intConfig("store.backup.thread.count", 1);
}
