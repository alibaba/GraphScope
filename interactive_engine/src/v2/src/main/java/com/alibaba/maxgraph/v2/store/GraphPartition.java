package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

public interface GraphPartition extends Closeable {

    /**
     * @param snapshotId
     * @param operationBatch
     * @return True if batch has DDL operation
     * @throws IOException
     */
    boolean writeBatch(long snapshotId, OperationBatch operationBatch) throws IOException;

    long recover();

    GraphDefPb getGraphDefBlob() throws IOException;

    void ingestHdfsFile(FileSystem fs, Path filePath) throws IOException;

    int getId();
}
