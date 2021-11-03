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

import com.alibaba.graphscope.groot.store.GraphPartitionBackup;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;

public class JnaGraphBackupEngine implements GraphPartitionBackup {
    private static final Logger logger = LoggerFactory.getLogger(JnaGraphBackupEngine.class);

    private Pointer bePointer;
    private int partitionId;
    private String backupPath;

    public JnaGraphBackupEngine(Pointer storePointer, int partitionId, String backupPath) {
        this.bePointer = GraphLibrary.INSTANCE.openGraphBackupEngine(storePointer, backupPath);
        this.partitionId = partitionId;
        this.backupPath = backupPath;
        logger.info("JNA store backup engine opened. partition [" + partitionId + "]");
    }

    @Override
    public void close() throws IOException {
        GraphLibrary.INSTANCE.closeGraphBackupEngine(this.bePointer);
    }

    @Override
    public int createNewPartitionBackup() throws IOException {
        try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.createNewBackup(this.bePointer)) {
            if (!jnaResponse.success()) {
                String errMsg = jnaResponse.getErrMsg();
                throw new IOException(errMsg);
            }
            byte[] data = jnaResponse.getData();
            if (data == null || data.length != Integer.BYTES) {
                throw new IOException("fail to get new created backup id from jna response, partition [" + this.partitionId + "]");
            }
            IntBuffer intBuf = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder()).asIntBuffer();
            int[] intData = new int[intBuf.remaining()];
            intBuf.get(intData);
            return intData[0];
        }
    }

    @Override
    public void restoreFromPartitionBackup(int partitionBackupId, String PartitionRestorePath) throws IOException {
        if (PartitionRestorePath.equals(this.backupPath)) {
            throw new IOException("restore path cannot be same with backup path");
        }
        try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.restoreFromBackup(
                this.bePointer, PartitionRestorePath, partitionBackupId)) {
            if (!jnaResponse.success()) {
                String errMsg = jnaResponse.getErrMsg();
                throw new IOException(errMsg);
            }
        }
    }

    @Override
    public void verifyPartitionBackup(int partitionBackupId) throws IOException {
        try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.verifyBackup(this.bePointer, partitionBackupId)) {
            if (!jnaResponse.success()) {
                String errMsg = jnaResponse.getErrMsg();
                throw new IOException(errMsg);
            }
        }
    }

    public void partitionBackupGc(List<Integer> readyPartitionBackupIds) throws IOException {
        if (readyPartitionBackupIds.isEmpty()) {
            return;
        }
        int[] partitionBackupIds;
        try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.getBackupList(this.bePointer)) {
            if (!jnaResponse.success()) {
                String errMsg = jnaResponse.getErrMsg();
                throw new IOException(errMsg);
            }
            byte[] data = jnaResponse.getData();
            if (data == null) {
                return;
            }
            IntBuffer intBuf = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder()).asIntBuffer();
            partitionBackupIds = new int[intBuf.remaining()];
            intBuf.get(partitionBackupIds);
        }
        for (int bId : partitionBackupIds) {
            if (!readyPartitionBackupIds.contains(bId)) {
                try (JnaResponse jnaResponse = GraphLibrary.INSTANCE.deleteBackup(this.bePointer, bId)) {
                    if (!jnaResponse.success()) {
                        logger.error("fail to delete backup [" + bId + "] from partition [" + this.partitionId + "], ignore");
                    }
                }
            }
        }
    }

    @Override
    public int getId() {
        return this.partitionId;
    }
}
