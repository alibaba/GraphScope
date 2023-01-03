package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
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
    private Configs configs;
    private ConcurrentHashMap<Integer, Long> hashMap;
    private RoleClients<CoordinatorSnapshotClient> clients;
    private ScheduledExecutorService updateStoreMinSnapshotScheduler;

    public GarbageCollectManager(Configs configs, RoleClients<CoordinatorSnapshotClient> clients) {
        this.configs = configs;
        this.hashMap = new ConcurrentHashMap<>();
        this.clients = clients;
    }

    public void put(int frontendId, long snapshotId) {
        hashMap.put(frontendId, snapshotId);
    }

    public void start() {
        this.updateStoreMinSnapshotScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "update-store-min-snapshot-scheduler", logger));
        this.updateStoreMinSnapshotScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        if (!hashMap.isEmpty()) {
                            long offlineVersion = Collections.min(hashMap.values()) - 1;
                            for (int i = 0; i < CommonConfig.STORE_NODE_COUNT.get(configs); i++) {
                                CoordinatorSnapshotClient client = clients.getClient(i);
                                client.synchronizeSnapshot(offlineVersion);
                                logger.info(
                                        "Offline version of store ["
                                                + i
                                                + "] updated to ["
                                                + offlineVersion
                                                + "]");
                            }
                        }
                    } catch (Exception e) {
                        logger.error("error in updateStoreMinSnapshotScheduler, ignore", e);
                    }
                },
                5000L,
                2000L,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (updateStoreMinSnapshotScheduler != null) {
            updateStoreMinSnapshotScheduler.shutdownNow();
            try {
                if (!updateStoreMinSnapshotScheduler.awaitTermination(
                        1000, TimeUnit.MILLISECONDS)) {
                    logger.error("updateStoreMinSnapshotScheduler await timeout before shutdown");
                }
            } catch (InterruptedException e) {
                logger.error("updateStoreMinSnapshotScheduler awaitTermination exception ", e);
            }
        }
    }
}
