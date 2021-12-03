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
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.common.config.BackupConfig;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.maxgraph.compiler.api.exception.BackupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BackupAgent {
    private static final Logger logger = LoggerFactory.getLogger(BackupAgent.class);

    private int storeId;
    private boolean backupEnable;
    private int backupThreadCount;
    private StoreService storeService;
    private Map<Integer, GraphPartitionBackup> idToPartitionBackup;
    private ExecutorService backupExecutor;

    public BackupAgent(Configs configs, StoreService storeService) {
        this.storeId = CommonConfig.NODE_IDX.get(configs);
        this.backupEnable = BackupConfig.BACKUP_ENABLE.get(configs);
        this.backupThreadCount = BackupConfig.STORE_BACKUP_THREAD_COUNT.get(configs);
        this.storeService = storeService;
    }

    public void start() {
        if (!this.backupEnable) {
            logger.info("store backup agent is disable, storeId [" + this.storeId + "]");
            return;
        }
        this.backupExecutor = new ThreadPoolExecutor(
                this.backupThreadCount,
                this.backupThreadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("store-backup", logger));
        Map<Integer, GraphPartition> idToPartition = storeService.getIdToPartition();
        this.idToPartitionBackup = new HashMap<>(idToPartition.size());
        for (Map.Entry<Integer, GraphPartition> entry : idToPartition.entrySet()) {
            this.idToPartitionBackup.put(entry.getKey(), entry.getValue().openBackupEngine());
        }
        logger.info("Store backup agent started. storeId [" + this.storeId + "]");
    }

    public void stop() {
        if (this.idToPartitionBackup != null) {
            CountDownLatch latch = new CountDownLatch(this.idToPartitionBackup.size());
            for (GraphPartitionBackup partitionBackup : this.idToPartitionBackup.values()) {
                this.backupExecutor.execute(() -> {
                    try {
                        partitionBackup.close();
                        logger.info("partition backup engine #[" + partitionBackup.getId() + "] closed");
                    } catch (IOException e) {
                        logger.error("partition backup engine #[" + partitionBackup.getId() + "] close failed", e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                long waitSeconds = 30L;
                if (!latch.await(waitSeconds, TimeUnit.SECONDS)) {
                    logger.warn("not all partition backup engines closed, waited [" + waitSeconds + "] seconds");
                }
            } catch (InterruptedException e) {
                // Ignore
            }
            this.idToPartitionBackup = null;
        }
        if (this.backupExecutor != null) {
            this.backupExecutor.shutdown();
            try {
                this.backupExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.backupExecutor = null;
        }
    }

    public void createNewStoreBackup(int globalBackupId, CompletionCallback<StoreBackupId> callback) {
        try {
            checkEnable();
        } catch (BackupException e) {
            callback.onError(e);
            return;
        }
        StoreBackupId storeBackupId = new StoreBackupId(globalBackupId);
        AtomicInteger counter = new AtomicInteger(this.idToPartitionBackup.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartitionBackup> entry : this.idToPartitionBackup.entrySet()) {
            this.backupExecutor.execute(() -> {
                if (finished.get()) {
                    return;
                }
                try {
                    int partitionId = entry.getKey();
                    int partitionBackupId = entry.getValue().createNewPartitionBackup();
                    storeBackupId.addPartitionBackupId(partitionId, partitionBackupId);
                    if (counter.decrementAndGet() == 0) {
                        callback.onCompleted(storeBackupId);
                    }
                } catch (Exception e) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    callback.onError(e);
                }
            });
        }
    }

    public void verifyStoreBackup(StoreBackupId storeBackupId, CompletionCallback<Void> callback) {
        try {
            checkEnable();
        } catch (BackupException e) {
            callback.onError(e);
            return;
        }
        if (storeBackupId.getPartitionToBackupId().size() != this.idToPartitionBackup.size()) {
            callback.onError(new BackupException(
                    "verifying from incorrect store backup id, globalBackupId #[" + storeBackupId.getGlobalBackupId() +
                            "], storeId #[" + this.storeId + "]"));
            return;
        }
        AtomicInteger counter = new AtomicInteger(this.idToPartitionBackup.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartitionBackup> entry : this.idToPartitionBackup.entrySet()) {
            this.backupExecutor.execute(() -> {
                if (finished.get()) {
                    return;
                }
                try {
                    int partitionId = entry.getKey();
                    entry.getValue().verifyPartitionBackup(storeBackupId.getPartitionToBackupId().get(partitionId));
                    if (counter.decrementAndGet() == 0) {
                        callback.onCompleted(null);
                    }
                } catch (Exception e) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    callback.onError(e);
                }
            });
        }
    }

    public void clearUnavailableStoreBackups(Map<Integer, List<Integer>> readyPartitionBackupIds,
                                             CompletionCallback<Void> callback) {
        try {
            checkEnable();
        } catch (BackupException e) {
            callback.onError(e);
            return;
        }
        if (readyPartitionBackupIds.size() != this.idToPartitionBackup.size()) {
            callback.onError(new BackupException("doing store backup up gc with incorrect ready partitionBackupId lists"));
            return;
        }
        AtomicInteger counter = new AtomicInteger(this.idToPartitionBackup.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartitionBackup> entry : this.idToPartitionBackup.entrySet()) {
            this.backupExecutor.execute(() -> {
                if (finished.get()) {
                    return;
                }
                try {
                    int partitionId = entry.getKey();
                    entry.getValue().partitionBackupGc(readyPartitionBackupIds.get(partitionId));
                    if (counter.decrementAndGet() == 0) {
                        callback.onCompleted(null);
                    }
                } catch (Exception e) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    callback.onError(e);
                }
            });
        }
    }

    public void restoreFromStoreBackup(StoreBackupId storeBackupId, String restoreRootPath,
                                       CompletionCallback<Void> callback) {
        try {
            checkEnable();
        } catch (BackupException e) {
            callback.onError(e);
            return;
        }
        if (storeBackupId.getPartitionToBackupId().size() != this.idToPartitionBackup.size()) {
            callback.onError(new BackupException(
                    "restoring from incorrect store backup id, globalBackupId #[" + storeBackupId.getGlobalBackupId() +
                    "], restoreRootPath: " + restoreRootPath + ", storeId #[" + this.storeId + "]"));
            return;
        }
        AtomicInteger counter = new AtomicInteger(this.idToPartitionBackup.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartitionBackup> entry : this.idToPartitionBackup.entrySet()) {
            this.backupExecutor.execute(() -> {
                if (finished.get()) {
                    return;
                }
                try {
                    int partitionId = entry.getKey();
                    Path partitionRestorePath = Paths.get(restoreRootPath, "" + partitionId);
                    if (!Files.isDirectory(partitionRestorePath)) {
                        Files.createDirectories(partitionRestorePath);
                    }
                    entry.getValue().restoreFromPartitionBackup(
                            storeBackupId.getPartitionToBackupId().get(partitionId), partitionRestorePath.toString());
                    if (counter.decrementAndGet() == 0) {
                        callback.onCompleted(null);
                    }
                } catch (Exception e) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    callback.onError(e);
                }
            });
        }
    }

    private void checkEnable() throws BackupException {
        if (!this.backupEnable) {
            throw new BackupException("store backup agent is disable now, storeId [" + this.storeId + "]");
        }
    }
}
