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
package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.common.util.PartitionUtils;
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
            partitionKey = keyBuf.getLong(8);
        } else {
            // Vertex
            partitionKey = keyBuf.getLong(8);
        }
        return PartitionUtils.getPartitionIdFromKey(partitionKey, numPartitions);
    }

}
