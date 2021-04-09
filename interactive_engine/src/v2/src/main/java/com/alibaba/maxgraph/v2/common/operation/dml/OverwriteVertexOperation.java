package com.alibaba.maxgraph.v2.common.operation.dml;

import com.alibaba.maxgraph.proto.v2.DataOperationPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.operation.VertexId;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.google.protobuf.ByteString;

import java.util.Map;

public class OverwriteVertexOperation extends Operation {

    private VertexId vertexId;
    private LabelId labelId;
    private Map<Integer, PropertyValue> properties;

    public OverwriteVertexOperation(VertexId vertexId, LabelId labelId, Map<Integer, PropertyValue> properties) {
        super(OperationType.OVERWRITE_VERTEX);
        this.vertexId = vertexId;
        this.labelId = labelId;
        this.properties = properties;
    }

    @Override
    protected long getPartitionKey() {
        return vertexId.getId();
    }

    @Override
    protected ByteString getBytes() {
        DataOperationPb.Builder builder = DataOperationPb.newBuilder();
        builder.setKeyBlob(vertexId.toProto().toByteString());
        builder.setLocationBlob(labelId.toProto().toByteString());
        properties.forEach((propertyId, val) -> builder.putProps(propertyId, val.toProto()));
        return builder.build().toByteString();
    }
}
