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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.store.StoreBackupId;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.maxgraph.compiler.api.exception.BackupException;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BackupManager {
    private static final Logger logger = LoggerFactory.getLogger(BackupManager.class);

    public static final String GLOBAL_BACKUP_ID_PATH = "global_backup_id";
    public static final String BACKUP_INFO_PATH = "backup_info";

    private MetaService metaService;
    private MetaStore metaStore;
    private SnapshotManager snapshotManager;
    private ObjectMapper objectMapper;

    private int storeNodeCount;
    private int graphPartitionCount;
    private StoreBackupTaskSender storeBackupTaskSender;

    private int backupCreationBufferMaxSize;
    private BlockingQueue<Integer> backupCreationBuffer;
    private int backupGcIntervalHours;
    private Boolean autoSubmit;
    private int autoSubmitIntervalHours;

    private ScheduledExecutorService backupCreationScheduler;
    private ScheduledExecutorService backupGcScheduler;
    private ScheduledExecutorService autoCommitScheduler;

    private volatile int globalBackupId;
    private Map<Integer, BackupInfo> globalBackupIdToInfo;

    private Lock globalBackupIdLock = new ReentrantLock();
    private Lock globalBackupIdToInfoLock = new ReentrantLock();

    public BackupManager(Configs configs, MetaService metaService, MetaStore metaStore, SnapshotManager snapshotManager,
                         StoreBackupTaskSender storeBackupTaskSender) {
        this.metaService = metaService;
        this.metaStore = metaStore;
        this.snapshotManager = snapshotManager;
        this.objectMapper = new ObjectMapper();

        this.storeNodeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.graphPartitionCount = this.metaService.getPartitionCount();
        this.storeBackupTaskSender = storeBackupTaskSender;

        this.backupCreationBufferMaxSize = CoordinatorConfig.BACKUP_CREATION_BUFFER_MAX_COUNT.get(configs);
        this.backupGcIntervalHours = CoordinatorConfig.BACKUP_GC_INTERVAL_HOURS.get(configs);
        this.autoSubmit = CoordinatorConfig.BACKUP_AUTO_SUBMIT.get(configs);
        this.autoSubmitIntervalHours = CoordinatorConfig.BACKUP_AUTO_SUBMIT_INTERVAL_HOURS.get(configs);
    }

    public void start() {
        try {
            recover();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }

        this.backupCreationBuffer = new ArrayBlockingQueue<>(backupCreationBufferMaxSize);
        this.backupCreationScheduler = Executors.newSingleThreadScheduledExecutor(
                ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("backup-creation-scheduler", logger));

        this.backupGcScheduler = Executors.newSingleThreadScheduledExecutor(
                ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("backup-gc-scheduler", logger));
        this.backupGcScheduler.scheduleWithFixedDelay(this::clearUnavailableBackups,
                backupGcIntervalHours, backupGcIntervalHours, TimeUnit.HOURS);

        if (autoSubmit) {
            this.autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(
                    ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("backup-autocommit-scheduler", logger));
            this.autoCommitScheduler.scheduleWithFixedDelay(() -> {
                try {
                    int newGlobalBackupId = createNewBackup();
                    logger.info("backup creation auto submitted with backupId #[" + newGlobalBackupId + "]");
                } catch (Exception e) {
                    logger.error("backup creation auto submit failed, ignore");
                }
            }, autoSubmitIntervalHours, autoSubmitIntervalHours, TimeUnit.HOURS);
        }
    }

    public void stop() {
        if (this.backupCreationScheduler != null) {
            this.backupCreationScheduler.shutdown();
            try {
                this.backupCreationScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.backupCreationScheduler = null;
        }
        if (this.backupGcScheduler != null) {
            this.backupGcScheduler.shutdown();
            try {
                this.backupGcScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.backupGcScheduler = null;
        }
        if (this.autoCommitScheduler != null) {
            this.autoCommitScheduler.shutdown();
            try {
                this.autoCommitScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.autoCommitScheduler = null;
        }
    }

    public int createNewBackup() throws IOException {
        int newGlobalBackupId = increaseGlobalBackupId();
        boolean suc = this.backupCreationBuffer.offer(newGlobalBackupId);
        if (!suc) {
            throw new BackupException("add backup creation task to buffer failed, new global backupId: " + newGlobalBackupId);
        }
        this.backupCreationScheduler.schedule(this::processBackupCreationTasks, 0L, TimeUnit.SECONDS);
        logger.info("submit new backup creation task with backupId #[" + newGlobalBackupId + "]");
        return newGlobalBackupId;
    }

    public void deleteBackup(int globalBackupId) throws IOException {
        if (!this.globalBackupIdToInfo.containsKey(globalBackupId)) {
            logger.warn("try to delete unavailable backup #[" + globalBackupId + "], ignore");
            return;
        }
        removeBackupInfo(globalBackupId);
        logger.info("backup #[" + globalBackupId + "] deleted");
    }

    public void purgeOldBackups(int keepAliveNum) throws IOException {
        if (keepAliveNum <= 0) {
            throw new IllegalArgumentException("the input keepAliveNum should > 0, got " + keepAliveNum);
        } else if (keepAliveNum >= this.globalBackupIdToInfo.size()) {
            return;
        }
        int numPurged = purgeOldBackupInfo(keepAliveNum);
        logger.info(numPurged + " old backups purged");
    }

    public void restoreFromLatest(String restoreRootPath) throws BackupException, ExecutionException, InterruptedException {
        int latestGlobalBackupId = getLatestGlobalBackupId();
        if (latestGlobalBackupId == -1) {
            throw new BackupException("no available backups to restore");
        }
        List<StoreBackupId> storeBackupIds = getStoreBackupIds(latestGlobalBackupId);
        AtomicInteger counter = new AtomicInteger(storeNodeCount);
        AtomicBoolean finished = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (int sId = 0; sId < storeNodeCount; sId++) {
            storeBackupTaskSender.restoreFromStoreBackup(sId, storeBackupIds.get(sId), restoreRootPath,
                    new CompletionCallback<Void>() {
                        @Override
                        public void onCompleted(Void res) {
                            if (!finished.get() && counter.decrementAndGet() == 0) {
                                future.complete(null);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            if (finished.getAndSet(true)) {
                                return;
                            }
                            future.completeExceptionally(t);
                        }
                    }
            );
        }
        future.get();
        logger.info("graph store restored from backup #[" + latestGlobalBackupId + "] at dir " + restoreRootPath + "");
    }

    public void verifyBackup(int globalBackupId) throws BackupException, ExecutionException, InterruptedException {
        if (!this.globalBackupIdToInfo.containsKey(globalBackupId)) {
            throw new BackupException("backup #[" + globalBackupId + "] not ready/existed");
        }
        List<StoreBackupId> storeBackupIds = getStoreBackupIds(globalBackupId);
        AtomicInteger counter = new AtomicInteger(storeNodeCount);
        AtomicBoolean finished = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (int sId = 0; sId < storeNodeCount; sId++) {
            storeBackupTaskSender.verifyStoreBackup(sId, storeBackupIds.get(sId), new CompletionCallback<Void>() {
                @Override
                public void onCompleted(Void res) {
                    if (!finished.get() && counter.decrementAndGet() == 0) {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    future.completeExceptionally(t);
                }
            });
        }
        future.get();
    }

    public List<BackupInfo> getBackupInfoList() {
        return new ArrayList<>(this.globalBackupIdToInfo.values());
    }

    private void checkMetaPath(String path) throws FileNotFoundException {
        if (!this.metaStore.exists(path)) {
            throw new FileNotFoundException(path);
        }
    }

    private void recover() throws IOException {
        checkMetaPath(GLOBAL_BACKUP_ID_PATH);
        checkMetaPath(BACKUP_INFO_PATH);

        byte[] globalBackupIdBytes = this.metaStore.read(GLOBAL_BACKUP_ID_PATH);
        this.globalBackupId = this.objectMapper.readValue(globalBackupIdBytes, Integer.class);

        byte[] backupInfoBytes = this.metaStore.read(BACKUP_INFO_PATH);
        List<BackupInfo> recoveredBackupInfoList = this.objectMapper.readValue(backupInfoBytes,
                new TypeReference<List<BackupInfo>>() {});
        this.globalBackupIdToInfo = new ConcurrentHashMap<>(recoveredBackupInfoList.size());
        for (BackupInfo bi : recoveredBackupInfoList) {
            this.globalBackupIdToInfo.put(bi.getGlobalBackupId(), bi);
        }
    }

    private void processBackupCreationTasks() {
        Integer newGlobalBackupId;
        while (true) {
            try {
                newGlobalBackupId = this.backupCreationBuffer.poll(1L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("polling backup creation buffer interrupted", e);
                // TODO: consider change to 'return'
                continue;
            }
            if (newGlobalBackupId == null) {
                return;
            }
            doBackupCreation(newGlobalBackupId);
        }
    }

    private void doBackupCreation(int newGlobalBackupId) {
        SnapshotInfo snapshotInfo = snapshotManager.getQuerySnapshotInfo();
        List<Long> walOffsets = snapshotManager.getQueueOffsets();
        Map<Integer, Integer> partitionToBackupId = new ConcurrentHashMap<>(graphPartitionCount);
        AtomicInteger counter = new AtomicInteger(storeNodeCount);
        AtomicBoolean finished = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (int sId = 0; sId < storeNodeCount; sId++) {
            storeBackupTaskSender.createStoreBackup(sId, newGlobalBackupId, new CompletionCallback<StoreBackupId>() {
                @Override
                public void onCompleted(StoreBackupId storeBackupId) {
                    if (finished.get()) {
                        return;
                    }
                    partitionToBackupId.putAll(storeBackupId.getPartitionToBackupId());
                    if (counter.decrementAndGet() == 0) {
                        if (partitionToBackupId.size() == graphPartitionCount) {
                            try {
                                addNewBackupInfo(new BackupInfo(newGlobalBackupId,
                                        snapshotInfo.getSnapshotId(), snapshotInfo.getDdlSnapshotId(),
                                        partitionToBackupId, walOffsets));
                                future.complete(null);
                            } catch (Exception e) {
                                future.completeExceptionally(new BackupException(
                                        "failed to persist backup info of new created backup #[" +
                                        newGlobalBackupId + "], " + e.getMessage()));
                            }
                        } else {
                            future.completeExceptionally(new BackupException(
                                    "got incorrect number of partition backupIds when creating backup #[" + newGlobalBackupId +
                                    "], got " + partitionToBackupId.size() + ", expect " + graphPartitionCount));
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    future.completeExceptionally(t);
                }
            });
        }
        try {
            future.get();
            logger.info("new backup [" + newGlobalBackupId + "] created");
        } catch (Exception e) {
            logger.error("create new backup [" + newGlobalBackupId + "] failed", e);
        }
    }

    private void clearUnavailableBackups() {
        List<Map<Integer, List<Integer>>> readyPartitionBackupIdsByStore = getPartitionReadyBackupIdsByStore();
        AtomicInteger counter = new AtomicInteger(storeNodeCount);
        AtomicBoolean finished = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (int sId = 0; sId < storeNodeCount; sId++) {
            storeBackupTaskSender.clearUnavailableBackups(sId, readyPartitionBackupIdsByStore.get(sId),
                    new CompletionCallback<Void>() {
                        @Override
                        public void onCompleted(Void res) {
                            if (finished.get()) {
                                return;
                            }
                            if (counter.decrementAndGet() == 0) {
                                future.complete(null);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            if (finished.getAndSet(true)) {
                                return;
                            }
                            future.completeExceptionally(t);
                        }
                    }
            );
        }
        try {
            future.get();
            logger.info("backup auto-gc task finished");
        } catch (Exception e) {
            logger.error("backup auto-gc task failed， ignore", e);
        }
    }

    private List<Map<Integer, List<Integer>>> getPartitionReadyBackupIdsByStore() {
        List<Map<Integer, List<Integer>>> ret = new ArrayList<>(storeNodeCount);
        for (int sId = 0; sId < storeNodeCount; sId++) {
            ret.add(new HashMap<>());
        }
        for (int pId = 0; pId < graphPartitionCount; pId++) {
            ret.get(metaService.getStoreIdByPartition(pId)).put(pId, new ArrayList<>());
        }
        for (Map.Entry<Integer, BackupInfo> globalBackupInfoEntry : this.globalBackupIdToInfo.entrySet()) {
            Map<Integer, Integer> partitionToBackupId = globalBackupInfoEntry.getValue().getPartitionToBackupId();
            if (partitionToBackupId.size() != graphPartitionCount) {
                logger.error("got error partition backup id list, global backup id: " + globalBackupInfoEntry.getKey());
                continue;
            }
            for (Map.Entry<Integer, Integer> partitionBackupMapEntry : partitionToBackupId.entrySet()) {
                int partitionId = partitionBackupMapEntry.getKey();
                int partitionBackupId = partitionBackupMapEntry.getValue();
                int storeId = metaService.getStoreIdByPartition(partitionId);
                ret.get(storeId).get(partitionId).add(partitionBackupId);
            }
        }
        return ret;
    }

    private int getLatestGlobalBackupId() {
        if (this.globalBackupIdToInfo.isEmpty()) {
            return -1;
        }
        return Collections.max(this.globalBackupIdToInfo.keySet());
    }

    private List<StoreBackupId> getStoreBackupIds(int globalBackupId) {
        List<StoreBackupId> storeBackupIds = new ArrayList<>(storeNodeCount);
        for (int sId = 0; sId < storeNodeCount; sId++) {
            storeBackupIds.add(new StoreBackupId(globalBackupId));
        }
        Map<Integer, Integer> partitionToBackupId = this.globalBackupIdToInfo.get(globalBackupId).getPartitionToBackupId();
        for (Map.Entry<Integer, Integer> entry : partitionToBackupId.entrySet()) {
            int partitionId = entry.getKey();
            int partitionBackupId = entry.getValue();
            int storeId = metaService.getStoreIdByPartition(partitionId);
            storeBackupIds.get(storeId).addPartitionBackupId(partitionId, partitionBackupId);
        }
        return storeBackupIds;
    }

    private int increaseGlobalBackupId() throws IOException {
        this.globalBackupIdLock.lock();
        try {
            int newGlobalBackupId = globalBackupId + 1;
            persistGlobalBackupId(newGlobalBackupId);
            this.globalBackupId = newGlobalBackupId;
            return this.globalBackupId;
        } finally {
            globalBackupIdLock.unlock();
        }
    }

    private void addNewBackupInfo(BackupInfo backupInfo) throws IOException {
        this.globalBackupIdToInfoLock.lock();
        try {
            List<BackupInfo> backupInfoList = new ArrayList<>(this.globalBackupIdToInfo.values());
            backupInfoList.add(backupInfo);
            persistBackupInfoList(backupInfoList);
            this.globalBackupIdToInfo.put(backupInfo.getGlobalBackupId(), backupInfo);
        } finally {
            globalBackupIdToInfoLock.unlock();
        }
    }

    private void removeBackupInfo(int globalBackupId) throws IOException {
        this.globalBackupIdToInfoLock.lock();
        try {
            List<BackupInfo> backupInfoList = new ArrayList<>(this.globalBackupIdToInfo.values());
            backupInfoList.removeIf(backupInfo -> backupInfo.getGlobalBackupId() == globalBackupId);
            persistBackupInfoList(backupInfoList);
            this.globalBackupIdToInfo.remove(globalBackupId);
        } finally {
            globalBackupIdToInfoLock.unlock();
        }
    }

    private int purgeOldBackupInfo(int keepAliveNum) throws IOException {
        this.globalBackupIdToInfoLock.lock();
        try  {
            List<BackupInfo> backupInfoList = new ArrayList<>(this.globalBackupIdToInfo.values());
            int oldSize = backupInfoList.size();
            int minBackupIdToKeep = backupInfoList.stream()
                    .mapToInt(BackupInfo::getGlobalBackupId)
                    .sorted()
                    .skip(oldSize - keepAliveNum)
                    .min()
                    .orElse(0);
            backupInfoList.removeIf(backupInfo -> backupInfo.getGlobalBackupId() < minBackupIdToKeep);
            persistBackupInfoList(backupInfoList);
            this.globalBackupIdToInfo.entrySet().removeIf(entry -> entry.getKey() < minBackupIdToKeep);
            return oldSize - keepAliveNum;
        } finally {
            globalBackupIdToInfoLock.unlock();
        }
    }

    private void persistGlobalBackupId(int newGlobalBackupId) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(newGlobalBackupId);
        this.metaStore.write(GLOBAL_BACKUP_ID_PATH, b);
    }

    private void persistBackupInfoList(List<BackupInfo> newBackupInfoList) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(newBackupInfoList);
        this.metaStore.write(BACKUP_INFO_PATH, b);
    }
}
