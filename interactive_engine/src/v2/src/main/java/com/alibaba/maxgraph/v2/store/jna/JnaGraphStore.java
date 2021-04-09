package com.alibaba.maxgraph.v2.store.jna;

import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.StoreConfig;
import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.sun.jna.Pointer;
import org.apache.hadoop.fs.FileSystem;
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

    public JnaGraphStore(Configs configs, int partitionId) throws IOException {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(configs);
        Path partitionPath = Paths.get(dataRoot, "" + partitionId);
        this.downloadPath = Paths.get(dataRoot, "download");
        if (!Files.isDirectory(partitionPath)) {
            Files.createDirectories(partitionPath);
        }
        if (!Files.isDirectory(downloadPath)) {
            Files.createDirectories(downloadPath);
        }
        Configs storeConfigs = Configs.newBuilder(configs)
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
        try (JnaResponse response = GraphLibrary.INSTANCE.writeBatch(this.pointer, snapshotId, dataBytes,
                dataBytes.length)) {
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
    public void ingestHdfsFile(FileSystem fs, org.apache.hadoop.fs.Path filePath) throws IOException {
        org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.Path(downloadPath.toString(),
                filePath.getName());
        fs.copyToLocalFile(filePath, targetPath);
        try (JnaResponse response = GraphLibrary.INSTANCE.ingestData(this.pointer, targetPath.toString())) {
            if (!response.success()) {
                throw new IOException(response.getErrMsg());
            }
        }
    }

    @Override
    public int getId() {
        return this.partitionId;
    }

}
