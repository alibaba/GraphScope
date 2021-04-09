package com.alibaba.maxgraph.v2.common.operation.dml;

import com.alibaba.maxgraph.proto.v2.DataOperationPb;
import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.google.protobuf.ByteString;

import java.util.Map;

public class OverwriteEdgeOperation extends Operation {

    private EdgeId edgeId;
    private EdgeKind edgeKind;
    private Map<Integer, PropertyValue> properties;

    public OverwriteEdgeOperation(EdgeId edgeId, EdgeKind edgeKind, Map<Integer, PropertyValue> properties) {
        super(OperationType.OVERWRITE_EDGE);
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
