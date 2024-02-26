package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

public class PartitionService {
    private static final Logger logger = LoggerFactory.getLogger(PartitionService.class);

    private ScheduledExecutorService scheduler;

    private final StoreService storeService;

    private final boolean isSecondary;
    private final long storeCatchupIntervalMS;

    public PartitionService(Configs configs, StoreService storeService) {
        this.storeService = storeService;
        this.isSecondary = CommonConfig.SECONDARY_INSTANCE_ENABLED.get(configs);
        this.storeCatchupIntervalMS = StoreConfig.STORE_GC_INTERVAL_MS.get(configs);
        // this.storeCatchupIntervalMS = StoreConfig.STORE_CATCHUP_INTERVAL_MS.get(configs);

        this.scheduler =
                Executors.newScheduledThreadPool(
                        1,
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "catch-up-scheduler", logger));
    }

    public void start() {
        if (isSecondary) {
            this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        try {
                            this.storeService.tryCatchUpWithPrimary();
                        } catch (IOException e) {
                            logger.error("try catch up with primary error", e);
                        }
                    },
                    storeCatchupIntervalMS,
                    storeCatchupIntervalMS,
                    TimeUnit.MILLISECONDS);
        }
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
    }
}
