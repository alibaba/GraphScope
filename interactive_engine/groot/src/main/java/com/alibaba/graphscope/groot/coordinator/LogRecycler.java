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

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
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

    private LogService logService;
    private SnapshotManager snapshotManager;
    private ScheduledExecutorService scheduler;
    private boolean recycleEnable;
    private long recycleIntervalSeconds;

    public LogRecycler(Configs configs, LogService logService, SnapshotManager snapshotManager) {
        this.logService = logService;
        this.snapshotManager = snapshotManager;
        this.recycleEnable = CoordinatorConfig.LOG_RECYCLE_ENABLE.get(configs);
        this.recycleIntervalSeconds = CoordinatorConfig.LOG_RECYCLE_INTERVAL_SECOND.get(configs);
    }

    public void start() {
        if (!this.recycleEnable) {
            logger.info("log recycler is disable");
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
        logger.info("LogRecycler stopped");
    }

    private void doRecycle() {
        List<Long> queueOffsets = this.snapshotManager.getQueueOffsets();
        for (int i = 0; i < queueOffsets.size(); i++) {
            try {
                logService.deleteBeforeOffset(i, queueOffsets.get(i));
                logger.info("recycled queue [" + i + "] offset [" + queueOffsets.get(i) + "]");
            } catch (IOException e) {
                logger.error(
                        "error in delete queue [" + i + "] offset [" + queueOffsets.get(i) + "]",
                        e);
            }
        }
    }
}
