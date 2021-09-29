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
package com.alibaba.maxgraph.groot.common.operation.ddl;

import com.alibaba.maxgraph.proto.v2.DdlOperationPb;
import com.alibaba.maxgraph.groot.common.schema.EdgeKind;
import com.alibaba.maxgraph.groot.common.operation.Operation;
import com.alibaba.maxgraph.groot.common.operation.OperationType;
import com.google.protobuf.ByteString;

public class RemoveEdgeKindOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private EdgeKind edgeKind;

    public RemoveEdgeKindOperation(int partitionId, long schemaVersion, EdgeKind edgeKind) {
        super(OperationType.REMOVE_EDGE_KIND);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.edgeKind = edgeKind;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(edgeKind.toProto().toByteString())
                .build()
                .toByteString();
    }
}
