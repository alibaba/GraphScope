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
package com.alibaba.graphscope.groot.operation.ddl;

import com.alibaba.maxgraph.proto.groot.DdlOperationPb;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.google.protobuf.ByteString;

public class DropVertexTypeOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private LabelId labelId;

    public DropVertexTypeOperation(int partitionId, long schemaVersion, LabelId labelId) {
        super(OperationType.DROP_VERTEX_TYPE);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.labelId = labelId;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(labelId.toProto().toByteString())
                .build()
                .toByteString();
    }
}
