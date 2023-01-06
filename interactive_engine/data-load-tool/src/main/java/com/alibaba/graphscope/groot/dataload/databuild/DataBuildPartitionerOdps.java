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
package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.sdkcommon.util.PartitionUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class DataBuildPartitionerOdps extends Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(DataBuildPartitionerOdps.class);

    @Override
    public int getPartition(Record key, Record value, int numPartitions) {
        byte[] keyBytes = null;
        try {
            keyBytes = ((String) key.get(0)).getBytes(DataBuildMapperOdps.charSet);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Should not have happened " + e.toString());
        }
        ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes);
        long partitionKey;
        if (keyBytes.length > 24) {
            // Edge
            partitionKey = keyBuf.getLong(8);
        } else {
            // Vertex
            partitionKey = keyBuf.getLong(8);
        }
        return PartitionUtils.getPartitionIdFromKey(partitionKey, numPartitions);
    }
}
