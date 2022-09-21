/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.dataload.graph;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Partitioner;

public class IrWriteGraphPartitioner extends Partitioner {
    @Override
    public int getPartition(Record key, Record value, int numPartitions) {
        long partitionKey = key.getBigint(0);
        return (int) PartitionUtils.getVertexPartition(partitionKey, numPartitions);
    }
}
