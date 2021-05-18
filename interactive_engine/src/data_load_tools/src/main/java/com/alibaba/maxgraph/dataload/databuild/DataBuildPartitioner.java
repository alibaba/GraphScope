package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.v2.common.util.PartitionUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class DataBuildPartitioner extends Partitioner<BytesWritable, BytesWritable> {

    private static final Logger logger = LoggerFactory.getLogger(DataBuildPartitioner.class);

    @Override
    public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
        byte[] keyBytes = key.getBytes();
        ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes);
        long partitionKey;
        if (key.getLength() > 24) {
            // Edge
            if ((keyBuf.getLong(0) & 1) == 1) {
                // In
                partitionKey = keyBuf.getLong(16);
            } else {
                // Out
                partitionKey = keyBuf.getLong(8);
            }
        } else {
            // Vertex
            partitionKey = keyBuf.getLong(8);
        }
        return PartitionUtils.getPartitionIdFromKey(partitionKey, numPartitions);
    }

}
