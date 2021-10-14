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
package com.alibaba.graphscope.groot.backup;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.groot.common.backup.BackupInfo;
import com.alibaba.maxgraph.groot.common.MetaService;
import com.alibaba.maxgraph.groot.common.rpc.RoleClients;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

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
    private RoleClients<StoreBackupClient> storeBackupClients;

    private int backupGcIntervalHours;
    private Boolean autocommit;
    private int autocommitIntervalHours;

    private ScheduledExecutorService backupGcScheduler;
    private ScheduledExecutorService autoCommitScheduler;

    private volatile int globalBackupId;
    private Map<Integer, BackupInfo> backupIdToInfo;

    public BackupManager(Configs configs, MetaService metaService, MetaStore metaStore, SnapshotManager snapshotManager,
                         RoleClients<StoreBackupClient> storeBackupClients) {
        this.metaService = metaService;
        this.metaStore = metaStore;
        this.snapshotManager = snapshotManager;
        this.objectMapper = new ObjectMapper();

        this.storeNodeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.graphPartitionCount = this.metaService.getPartitionCount();
        this.storeBackupClients = storeBackupClients;

        this.backupGcIntervalHours = StoreConfig.STORE_BACKUP_GC_INTERVAL_HOURS.get(configs);
        this.autocommit = CoordinatorConfig.BACKUP_AUTOCOMMIT.get(configs);
        this.autocommitIntervalHours = CoordinatorConfig.BACKUP_AUTOCOMMIT_INTERVAL_HOURS.get(configs);
    }

    public void start() {
        try {
            recover();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }

        this.backupGcScheduler = Executors.newSingleThreadScheduledExecutor(
                ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("backup-gc-scheduler", logger));
        this.backupGcScheduler.scheduleWithFixedDelay(() -> {
            deleteUnavailableBackups();
            logger.info("backup auto-gc finished");
        }, backupGcIntervalHours, backupGcIntervalHours, TimeUnit.HOURS);

        if (autocommit) {
            this.autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(
                    ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler("backup-autocommit-scheduler", logger));
            this.autoCommitScheduler.scheduleWithFixedDelay(() -> {
                int newBackupId = createNewBackup();
                logger.info("backup " + newBackupId + " auto created");
            }, autocommitIntervalHours, autocommitIntervalHours, TimeUnit.HOURS);
        }
    }

    public void stop() {
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

    public int createNewBackup() {
        return 0;
    }

    public void deleteBackup(int backupId) {
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
        int recoveredGlobalBackupId = this.objectMapper.readValue(globalBackupIdBytes, Integer.class);

        byte[] backupInfoBytes = this.metaStore.read(BACKUP_INFO_PATH);
        Map<Integer, BackupInfo> recoveredBackupInfo = this.objectMapper.readValue(backupInfoBytes,
                new TypeReference<Map<Integer, BackupInfo>>() {});

        this.globalBackupId = recoveredGlobalBackupId;
        this.backupIdToInfo = new ConcurrentHashMap<>(recoveredBackupInfo);
    }

    private void deleteUnavailableBackups() {

    }

    private void persistGlobalBackupId(int newGlobalBackupId) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(newGlobalBackupId);
        this.metaStore.write(GLOBAL_BACKUP_ID_PATH, b);
    }

    private void persistBackupInfoMap(Map<Integer, BackupInfo> newBackupIdToInfo) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(newBackupIdToInfo);
        this.metaStore.write(BACKUP_INFO_PATH, b);
    }
}
