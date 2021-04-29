package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.proto.v2.DataLoadTargetPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.PrepareDataLoadOperation;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

public class PrepareDataLoadExecutor extends AbstractDdlExecutor {
    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount) throws InvalidProtocolBufferException {
        DataLoadTargetPb dataLoadTargetPb = DataLoadTargetPb.parseFrom(ddlBlob);
        DataLoadTarget dataLoadTarget = DataLoadTarget.parseProto(dataLoadTargetPb);
        String label = dataLoadTarget.getLabel();
        String srcLabel = dataLoadTarget.getSrcLabel();
        String dstLabel = dataLoadTarget.getDstLabel();

        long version = graphDef.getSchemaVersion();
        if (!graphDef.hasLabel(label)) {
            throw new DdlException("label [" + label + "] not exists, schema version [" + version + "]");
        }

        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        graphDefBuilder.setVersion(version + 1);
        TypeDef typeDef = graphDef.getTypeDef(label);
        long tableIdx = graphDef.getTableIdx();
        tableIdx++;
        if (srcLabel == null || srcLabel.isEmpty()) {
            // Vertex type
            if (typeDef.getTypeEnum() != TypeEnum.VERTEX) {
                throw new DdlException("invalid data load target [" + dataLoadTarget + "], label is not a vertex");
            }
            graphDefBuilder.putVertexTableId(typeDef.getTypeLabelId(), tableIdx);
            graphDefBuilder.setTableIdx(tableIdx);
        } else {
            // Edge kind
            if (typeDef.getTypeEnum() != TypeEnum.EDGE) {
                throw new DdlException("invalid data load target [" + dataLoadTarget + "], label is not an edge");
            }
            EdgeKind.Builder edgeKindBuilder = EdgeKind.newBuilder();
            LabelId edgeLabelId = graphDef.getLabelId(label);
            if (edgeLabelId == null) {
                throw new DdlException("invalid edgeLabel [" + label + "], schema version [" + version + "]");
            }
            edgeKindBuilder.setEdgeLabelId(edgeLabelId);
            LabelId srcVertexLabelId = graphDef.getLabelId(srcLabel);
            if (srcVertexLabelId == null) {
                throw new DdlException("invalid srcVertexLabel [" + srcLabel + "], schema version [" + version + "]");
            }
            edgeKindBuilder.setSrcVertexLabelId(srcVertexLabelId);
            LabelId dstVertexLabelId = graphDef.getLabelId(dstLabel);
            if (dstVertexLabelId == null) {
                throw new DdlException("invalid dstVertexLabel [" + dstLabel + "], schema version [" + version + "]");
            }
            edgeKindBuilder.setDstVertexLabelId(dstVertexLabelId);
            EdgeKind edgeKind = edgeKindBuilder.build();
            if (!graphDef.hasEdgeKind(edgeKind)) {
                throw new DdlException("invalid data load target [" + dataLoadTarget + "], edgeKind not exists");
            }
            graphDefBuilder.putEdgeTableId(edgeKind, tableIdx);
            graphDefBuilder.setTableIdx(tableIdx);
        }

        GraphDef newGraphDef = graphDefBuilder.build();
        List<Operation> operations = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            operations.add(new PrepareDataLoadOperation(i, version, dataLoadTarget, tableIdx));
        }
        return new DdlResult(newGraphDef, operations);
    }
}
