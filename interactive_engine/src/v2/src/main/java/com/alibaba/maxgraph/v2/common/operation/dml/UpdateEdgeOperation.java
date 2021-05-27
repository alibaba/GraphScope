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
package com.alibaba.maxgraph.v2.common.operation.dml;

import com.alibaba.maxgraph.proto.v2.DataOperationPb;
import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.google.protobuf.ByteString;

import java.util.Map;

public class UpdateEdgeOperation extends Operation {

    private EdgeId edgeId;
    private EdgeKind edgeKind;
    private Map<Integer, PropertyValue> properties;

    public UpdateEdgeOperation(EdgeId edgeId, EdgeKind edgeKind, Map<Integer, PropertyValue> properties) {
        super(OperationType.UPDATE_EDGE);
        this.edgeId = edgeId;
        this.edgeKind = edgeKind;
        this.properties = properties;
    }

    @Override
    protected long getPartitionKey() {
        return edgeId.getSrcId().getId();
    }

    @Override
    protected ByteString getBytes() {
        DataOperationPb.Builder builder = DataOperationPb.newBuilder();
        builder.setKeyBlob(edgeId.toProto().toByteString());
        builder.setLocationBlob(edgeKind.toOperationProto().toByteString());
        properties.forEach((propertyId, val) -> builder.putProps(propertyId, val.toProto()));
        return builder.build().toByteString();
    }
}
