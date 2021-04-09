package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.operation.Operation;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBlob;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DdlExecutors {

    private Map<OperationType, AbstractDdlExecutor> ddlExecutors;

    public DdlExecutors() {
        this.ddlExecutors = new HashMap<>();
        ddlExecutors.put(OperationType.CREATE_VERTEX_TYPE, new CreateVertexTypeExecutor());
        ddlExecutors.put(OperationType.CREATE_EDGE_TYPE, new CreateEdgeTypeExecutor());
        ddlExecutors.put(OperationType.DROP_VERTEX_TYPE, new DropVertexTypeExecutor());
        ddlExecutors.put(OperationType.DROP_EDGE_TYPE, new DropEdgeTypeExecutor());
        ddlExecutors.put(OperationType.ADD_EDGE_KIND, new AddEdgeKindExecutor());
        ddlExecutors.put(OperationType.REMOVE_EDGE_KIND, new RemoveEdgeKindExecutor());
    }

    public AbstractDdlExecutor getExecutor(OperationType operationType) {
        AbstractDdlExecutor ddlExecutor = this.ddlExecutors.get(operationType);
        if (ddlExecutor != null) {
            return ddlExecutor;
        }
        throw new UnsupportedOperationException("No executor for operation [" + operationType + "]");
    }

    public DdlResult executeDdlRequestBatch(DdlRequestBatch ddlRequestBatch, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        List<Operation> operations = new ArrayList<>();
        GraphDef tmpGraphDef = graphDef;
        for (DdlRequestBlob ddlRequestBlob : ddlRequestBatch) {
            OperationType operationType = ddlRequestBlob.getOperationType();
            ByteString ddlBlob = ddlRequestBlob.getBytes();
            DdlResult ddlResult = getExecutor(operationType).execute(ddlBlob, tmpGraphDef, partitionCount);
            operations.addAll(ddlResult.getDdlOperations());
            tmpGraphDef = ddlResult.getGraphDef();
        }
        return new DdlResult(tmpGraphDef, operations);
    }

}
