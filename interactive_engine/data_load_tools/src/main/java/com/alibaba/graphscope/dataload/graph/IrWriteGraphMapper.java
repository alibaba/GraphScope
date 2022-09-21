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

import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

public class IrWriteGraphMapper extends MapperBase {
    private Record key;
    private int partitions;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.key = context.createMapOutputKeyRecord();
        this.partitions = Integer.valueOf(context.getJobConf().get(IrDataBuild.GRAPH_REDUCER_NUM));
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String tableName = context.getInputTableInfo().getTableName();
        if (tableName.contains(IrDataBuild.ENCODE_VERTEX_MAGIC)) { // vertex
            IrVertexData vertexData = new IrVertexData();
            vertexData.readRecord(record);
            this.key.setBigint(0, vertexData.id);
            this.key.setBigint(1, 0l); // 0 -> vertex type
            context.write(this.key, record);
        } else { // edge
            IrEdgeData edgeData = new IrEdgeData();
            edgeData.readRecord(record);
            // write edges according to src vertex
            this.key.setBigint(0, edgeData.srcVertexId);
            this.key.setBigint(1, 1l); // 1 -> edge type
            context.write(this.key, record);
            if (!isSrcDstSamePartition(edgeData)) {
                // write edges according to dst vertex
                this.key.setBigint(0, edgeData.dstVertexId);
                this.key.setBigint(1, 1l); // 1 -> edge type
                context.write(this.key, record);
            }
        }
    }

    private boolean isSrcDstSamePartition(IrEdgeData edgeData) {
        long srcId = edgeData.srcVertexId;
        long dstId = edgeData.dstVertexId;
        return PartitionUtils.getVertexPartition(srcId, partitions)
                == PartitionUtils.getVertexPartition(dstId, partitions);
    }
}
