package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.proto.v2.EdgeKindPb;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.ddl.AddEdgeKindOperation;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;

public class AddEdgeKindExecutor extends AbstractDdlExecutor {

    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {

        EdgeKindPb edgeKindPb = EdgeKindPb.parseFrom(ddlBlob);
        EdgeKind edgeKind = EdgeKind.parseProto(edgeKindPb);
        long version = graphDef.getSchemaVersion();

        EdgeKind.Builder edgeKindBuilder = EdgeKind.newBuilder(edgeKind);
        String edgeLabel = edgeKind.getEdgeLabel();
        LabelId edgeLabelId = graphDef.getLabelId(edgeLabel);
        if (edgeLabelId == null) {
            throw new DdlException("invalid edgeLabel [" + edgeLabel + "], schema version [" + version + "]");
        }
        edgeKindBuilder.setEdgeLabelId(edgeLabelId);

        String srcVertexLabel = edgeKind.getSrcVertexLabel();
        LabelId srcVertexLabelId = graphDef.getLabelId(srcVertexLabel);
        if (srcVertexLabelId == null) {
            throw new DdlException("invalid srcVertexLabel [" + srcVertexLabel + "], schema version [" + version + "]");
        }
        edgeKindBuilder.setSrcVertexLabelId(srcVertexLabelId);

        String dstVertexLabel = edgeKind.getDstVertexLabel();
        LabelId dstVertexLabelId = graphDef.getLabelId(dstVertexLabel);
        if (dstVertexLabelId == null) {
            throw new DdlException("invalid dstVertexLabel [" + dstVertexLabel + "], schema version [" + version + "]");
        }
        edgeKindBuilder.setDstVertexLabelId(dstVertexLabelId);

        EdgeKind newEdgeKind = edgeKindBuilder.build();
        if (graphDef.hasEdgeKind(newEdgeKind)) {
            throw new DdlException("edgeKind [" + newEdgeKind + "] already exists, schema version [" + version + "]");
        }

        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        version++;
        graphDefBuilder.setVersion(version);
        graphDefBuilder.addEdgeKind(newEdgeKind);
        long tableIdx = graphDef.getTableIdx();
        tableIdx++;
        graphDefBuilder.putEdgeTableId(newEdgeKind, tableIdx);
        graphDefBuilder.setTableIdx(tableIdx);
        GraphDef newGraphDef = graphDefBuilder.build();

        List<Operation> operations = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            operations.add(new AddEdgeKindOperation(i, version, newEdgeKind, tableIdx));
        }
        return new DdlResult(newGraphDef, operations);
    }

}
