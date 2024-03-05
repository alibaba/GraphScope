package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GarbageCollectManager {
    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectManager.class);
    private final Configs configs;
    private final ConcurrentHashMap<Integer, Long> hashMap;
    private final RoleClients<CoordinatorSnapshotClient> clients;
    private ScheduledExecutorService updateScheduler;
    private final long interval;

    public GarbageCollectManager(Configs configs, RoleClients<CoordinatorSnapshotClient> clients) {
        this.configs = configs;
        this.hashMap = new ConcurrentHashMap<>();
        this.clients = clients;
        this.interval = StoreConfig.STORE_GC_INTERVAL_MS.get(configs);
    }

    public void put(int frontendId, long snapshotId) {
        hashMap.put(frontendId, snapshotId);
    }

    public void start() {
        this.updateScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "update-store-min-snapshot-scheduler", logger));
        this.updateScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        if (!hashMap.isEmpty()) {
                            long offlineVersion = Collections.min(hashMap.values()) - 1;
                            for (int i = 0; i < CommonConfig.STORE_NODE_COUNT.get(configs); i++) {
                                CoordinatorSnapshotClient client = clients.getClient(i);
                                client.synchronizeSnapshot(offlineVersion);
                                if (i == 0 && offlineVersion % 1000 == 0) {
                                    logger.info("Offline version updated to {}", offlineVersion);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(
                                "error in updateStoreMinSnapshotScheduler {}, ignore",
                                e.getMessage());
                    }
                },
                interval,
                interval,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (updateScheduler != null) {
            updateScheduler.shutdownNow();
            try {
                if (!updateScheduler.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    logger.error("updateStoreMinSnapshotScheduler await timeout before shutdown");
                }
            } catch (InterruptedException e) {
                logger.error("updateStoreMinSnapshotScheduler awaitTermination exception ", e);
            }
        }
    }
}
