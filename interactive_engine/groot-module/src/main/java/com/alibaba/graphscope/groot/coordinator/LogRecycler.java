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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.CoordinatorConfig;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.wal.LogService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogRecycler {
    private static final Logger logger = LoggerFactory.getLogger(LogRecycler.class);

    private final LogService logService;
    private final SnapshotManager snapshotManager;
    private ScheduledExecutorService scheduler;
    private final boolean recycleEnable;
    private final long recycleIntervalSeconds;
    private final long recycleOffsetReserve;

    public LogRecycler(Configs configs, LogService logService, SnapshotManager snapshotManager) {
        this.logService = logService;
        this.snapshotManager = snapshotManager;
        this.recycleEnable = CoordinatorConfig.LOG_RECYCLE_ENABLE.get(configs);
        this.recycleIntervalSeconds = CoordinatorConfig.LOG_RECYCLE_INTERVAL_SECOND.get(configs);
        this.recycleOffsetReserve = CoordinatorConfig.LOG_RECYCLE_OFFSET_RESERVE.get(configs);
    }

    public void start() {
        if (!this.recycleEnable) {
            logger.info("log recycler is disabled");
            return;
        }
        this.scheduler =
                Executors.newScheduledThreadPool(
                        1,
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "log-recycler", logger));
        this.scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        doRecycle();
                    } catch (Exception e) {
                        logger.error("recycle error", e);
                    }
                },
                recycleIntervalSeconds,
                recycleIntervalSeconds,
                TimeUnit.SECONDS);
        logger.info("LogRecycler started");
    }

    public void stop() {
        if (scheduler != null) {
            this.scheduler.shutdown();
            try {
                this.scheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.scheduler = null;
        }
        logger.debug("LogRecycler stopped");
    }

    private void doRecycle() {
        List<Long> queueOffsets = this.snapshotManager.getQueueOffsets();
        for (int i = 0; i < queueOffsets.size(); i++) {
            long offset = queueOffsets.get(i);
            offset = Math.max(offset - recycleOffsetReserve, 0); // Leave some spaces
            try {
                logService.deleteBeforeOffset(i, offset);
                logger.info("recycled queue [{}] offset [{}]", i, offset);
            } catch (IOException e) {
                logger.error("error in delete queue [{}] offset [{}]", i, offset, e);
            }
        }
    }
}
