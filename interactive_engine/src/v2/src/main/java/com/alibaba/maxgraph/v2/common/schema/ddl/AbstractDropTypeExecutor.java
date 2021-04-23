package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.proto.v2.TypeDefPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractDropTypeExecutor extends AbstractDdlExecutor {

    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        TypeDefPb typeDefPb = TypeDefPb.parseFrom(ddlBlob);
        TypeDef typeDef = TypeDef.parseProto(typeDefPb);
        long version = graphDef.getSchemaVersion();
        String label = typeDef.getLabel();
        if (!graphDef.hasLabel(label)) {
            throw new DdlException("label [" + label + "] not exists, schema version [" + version + "]");
        }
        LabelId labelId = graphDef.getLabelId(label);
        Set<EdgeKind> edgeKindSet = graphDef.getIdToKinds().get(labelId);
        if (edgeKindSet != null && edgeKindSet.size() > 0) {
            throw new DdlException("cannot drop label [" + label + "], since it has related edgeKinds");
        }
        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        version++;
        graphDefBuilder.setVersion(version);
        graphDefBuilder.removeTypeDef(label);

        Set<String> remainPropertyNames = new HashSet<>();
        for (TypeDef remainTypeDef : graphDefBuilder.getAllTypeDefs()) {
            for (PropertyDef property : remainTypeDef.getProperties()) {
                remainPropertyNames.add(property.getName());
            }
        }
        graphDefBuilder.clearUnusedPropertyName(remainPropertyNames);

        GraphDef newGraphDef = graphDefBuilder.build();

        List<Operation> operations = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            operations.add(makeOperation(i, version, labelId));
        }
        return new DdlResult(newGraphDef, operations);
    }

    protected abstract Operation makeOperation(int partition, long version, LabelId labelId);
}
