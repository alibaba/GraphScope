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
package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBlob;
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
        ddlExecutors.put(OperationType.PREPARE_DATA_LOAD, new PrepareDataLoadExecutor());
        ddlExecutors.put(OperationType.COMMIT_DATA_LOAD, new CommitDataLoadExecutor());
    }

    public AbstractDdlExecutor getExecutor(OperationType operationType) {
        AbstractDdlExecutor ddlExecutor = this.ddlExecutors.get(operationType);
        if (ddlExecutor != null) {
            return ddlExecutor;
        }
        throw new UnsupportedOperationException(
                "No executor for operation [" + operationType + "]");
    }

    public DdlResult executeDdlRequestBatch(
            DdlRequestBatch ddlRequestBatch, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        List<Operation> operations = new ArrayList<>();
        GraphDef tmpGraphDef = graphDef;
        for (DdlRequestBlob ddlRequestBlob : ddlRequestBatch) {
            OperationType operationType = ddlRequestBlob.getOperationType();
            ByteString ddlBlob = ddlRequestBlob.getBytes();
            DdlResult ddlResult =
                    getExecutor(operationType).execute(ddlBlob, tmpGraphDef, partitionCount);
            operations.addAll(ddlResult.getDdlOperations());
            tmpGraphDef = ddlResult.getGraphDef();
        }
        return new DdlResult(tmpGraphDef, operations);
    }
}
