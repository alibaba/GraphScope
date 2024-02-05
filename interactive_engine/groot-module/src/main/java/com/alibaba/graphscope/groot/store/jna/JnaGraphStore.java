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
package com.alibaba.graphscope.groot.store.jna;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.GraphPartitionBackup;
import com.alibaba.graphscope.groot.store.external.ExternalStorage;
import com.alibaba.graphscope.proto.groot.GraphDefPb;
import com.sun.jna.Pointer;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JnaGraphStore implements GraphPartition {
    private static final Logger logger = LoggerFactory.getLogger(JnaGraphStore.class);

    private Pointer pointer;
    private int partitionId;
    private Path downloadPath;
    private Path backupPath;

    public JnaGraphStore(Configs configs, int partitionId) throws IOException {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(configs);
        Path partitionPath = Paths.get(dataRoot, "" + partitionId);
        this.downloadPath = Paths.get(dataRoot, "download");
        this.backupPath = Paths.get(dataRoot, "backups", "" + partitionId);
        String secondaryDataRoot = StoreConfig.STORE_SECONDARY_DATA_PATH.get(configs);
        Path secondPath = Paths.get(secondaryDataRoot, "" + partitionId);
        if (!Files.isDirectory(secondPath)) {
            Files.createDirectories(secondPath);
        }
        if (!Files.isDirectory(partitionPath)) {
            Files.createDirectories(partitionPath);
        }
        // Recreate download directory to clear previous junk files
        if (Files.isDirectory(downloadPath)) {
            FileUtils.forceDelete(downloadPath.toFile());
        }
        Files.createDirectories(downloadPath);
        if (!Files.isDirectory(backupPath)) {
            Files.createDirectories(backupPath);
        }
        Configs.Builder builder = Configs.newBuilder(configs);
        builder.put(StoreConfig.STORE_DATA_PATH.getKey(), partitionPath.toString());
        builder.put(StoreConfig.STORE_SECONDARY_DATA_PATH.getKey(), secondPath.toString());
        byte[] configBytes = builder.build().toProto().toByteArray();
        this.pointer = GraphLibrary.INSTANCE.openGraphStore(configBytes, configBytes.length);
        this.partitionId = partitionId;
        logger.info("JNA store opened. partition [" + partitionId + "]");
    }

    public Pointer getPointer() {
        return this.pointer;
    }

    @Override
    public void close() throws IOException {
        GraphLibrary.INSTANCE.closeGraphStore(this.pointer);
    }

    @Override
    public boolean writeBatch(long snapshotId, OperationBatch operationBatch) throws IOException {
        byte[] dataBytes = operationBatch.toProto().toByteArray();
        try (JnaResponse response =
                GraphLibrary.INSTANCE.writeBatch(
                        this.pointer, snapshotId, dataBytes, dataBytes.length)) {
            if (!response.success()) {
                String errMsg = response.getErrMsg();
                throw new IOException(errMsg);
            }
            return response.hasDdl();
        }
    }

    @Override
    public GraphDefPb getGraphDefBlob() throws IOException {
        try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.getGraphDefBlob(this.pointer)) {
            if (!jnaResponse.success()) {
                String errMsg = jnaResponse.getErrMsg();
                throw new IOException(errMsg);
            }
            return GraphDefPb.parseFrom(jnaResponse.getData());
        }
    }

    @Override
    public void ingestExternalFile(ExternalStorage storage, String sstPath) throws IOException {
        String[] items = sstPath.split("/");
        String unique_path = items[items.length - 2];
        String sstName = sstPath.substring(sstPath.lastIndexOf('/') + 1);
        String sstLocalPath = downloadPath.toString() + "/" + unique_path + "/" + sstName;
        storage.downloadDataWithRetry(sstPath, sstLocalPath);
    }

    @Override
    public GraphPartitionBackup openBackupEngine() {
        return new JnaGraphBackupEngine(this.pointer, this.partitionId, this.backupPath.toString());
    }

    @Override
    public int getId() {
        return this.partitionId;
    }

    @Override
    public void garbageCollect(long snapshotId) throws IOException {
        ensurePointer();
        try (JnaResponse response =
                GraphLibrary.INSTANCE.garbageCollectSnapshot(this.pointer, snapshotId)) {
            if (!response.success()) {
                throw new IOException(response.getErrMsg());
            }
        }
    }

    @Override
    public void tryCatchUpWithPrimary() throws IOException {
        ensurePointer();
        try (JnaResponse response = GraphLibrary.INSTANCE.tryCatchUpWithPrimary(this.pointer)) {
            if (!response.success()) {
                throw new IOException(response.getErrMsg());
            }
        }
    }

    @Override
    public void reopenSecondary(long wait_sec) throws IOException {
        ensurePointer();
        try (JnaResponse response = GraphLibrary.INSTANCE.reopenSecondary(this.pointer, wait_sec)) {
            if (!response.success()) {
                throw new IOException(response.getErrMsg());
            }
        }
    }

    private void ensurePointer() throws IOException {
        if (this.pointer == null) {
            throw new IOException("JNA pointer is null");
        }
    }
}
