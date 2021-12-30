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

import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.schema.TypeDef;
import com.alibaba.maxgraph.proto.groot.CreateVertexTypePb;
import com.alibaba.maxgraph.proto.groot.DdlOperationPb;
import com.google.protobuf.ByteString;

public class CreateVertexTypeOperation extends Operation {

    private int partitionId;
    private long schemaVersion;
    private TypeDef typeDef;
    private long tableIdx;

    public CreateVertexTypeOperation(
            int partitionId, long schemaVersion, TypeDef typeDef, long tableIdx) {
        super(OperationType.CREATE_VERTEX_TYPE);
        this.partitionId = partitionId;
        this.schemaVersion = schemaVersion;
        this.typeDef = typeDef;
        this.tableIdx = tableIdx;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getSchemaVersion() {
        return schemaVersion;
    }

    public TypeDef getTypeDef() {
        return typeDef;
    }

    @Override
    protected long getPartitionKey() {
        return partitionId;
    }

    @Override
    protected ByteString getBytes() {
        return DdlOperationPb.newBuilder()
                .setSchemaVersion(schemaVersion)
                .setDdlBlob(
                        CreateVertexTypePb.newBuilder()
                                .setTypeDef(typeDef.toProto())
                                .setTableIdx(tableIdx)
                                .build()
                                .toByteString())
                .build()
                .toByteString();
    }
}
