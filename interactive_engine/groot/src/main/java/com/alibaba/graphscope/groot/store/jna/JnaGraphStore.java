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

import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.GraphPartitionBackup;
import com.alibaba.graphscope.groot.store.external.ExternalStorage;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.maxgraph.proto.groot.GraphDefPb;
import com.sun.jna.Pointer;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
        if (!Files.isDirectory(partitionPath)) {
            Files.createDirectories(partitionPath);
        }
        if (!Files.isDirectory(downloadPath)) {
            Files.createDirectories(downloadPath);
        }
        if (!Files.isDirectory(backupPath)) {
            Files.createDirectories(backupPath);
        }
        Configs storeConfigs =
                Configs.newBuilder(configs)
                        .put("store.data.path", partitionPath.toString())
                        .build();
        byte[] configBytes = storeConfigs.toProto().toByteArray();
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
    public long recover() {
        return 0L;
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
        String[] items = sstPath.split("\\/");
        String unique_path = items[items.length - 2];
        String sstName = sstPath.substring(sstPath.lastIndexOf('/') + 1);
        String sstLocalPath = downloadPath.toString() + "/" + unique_path + "/" + sstName;
        URI uri = URI.create(sstPath);
        String scheme = uri.getScheme();
        if ("oss".equals(scheme)) {
            String chkPath = sstPath.substring(0, sstPath.length() - ".sst".length()) + ".chk";
            String chkName = sstName.substring(0, sstName.length() - ".sst".length()) + ".chk";
            String chkLocalPath = downloadPath.toString() + "/" + unique_path + "/" + chkName;

            storage.downloadData(chkPath, chkLocalPath);
            File file = new File(chkLocalPath);
            byte[] chkData = new byte[(int) file.length()];
            try {
                FileInputStream fis = new FileInputStream(file);
                fis.read(chkData);
                fis.close();
            } catch (FileNotFoundException e) {
                throw new IOException(e);
            } catch (IOException e) {
                throw e;
            }
            String[] chkArray = new String(chkData).split(",");
            if ("0".equals(chkArray[0])) {
                return;
            }
            String chkMD5Value = chkArray[1];
            storage.downloadData(sstPath, sstLocalPath);
            String sstMD5Value = getFileMD5(sstLocalPath);
            if (!chkMD5Value.equals(sstMD5Value)) {
                throw new IOException("CheckSum failed!");
            }
        } else if ("hdfs".equals(scheme)) {
            storage.downloadData(sstPath, sstLocalPath);
        } else {
            throw new IllegalArgumentException(
                    "external storage scheme [" + scheme + "] not supported");
        }
    }

    public String getFileMD5(String fileName) throws IOException {
        FileInputStream fis = null;
        try {
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            fis = new FileInputStream(fileName);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fis.read(buffer)) != -1) {
                MD5.update(buffer, 0, length);
            }
            return new String(Hex.encodeHex(MD5.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                throw e;
            }
        }
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
        try (JnaResponse response =
                GraphLibrary.INSTANCE.garbageCollectSnapshot(this.pointer, snapshotId)) {
            if (!response.success()) {
                throw new IOException(response.getErrMsg());
            }
        }
    }
}
