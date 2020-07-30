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
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.graph.MaxGraphSource;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.graph.OdpsGraph;
import com.alibaba.maxgraph.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.google.common.collect.Sets;

import java.util.stream.Collectors;

public class GraphSourceTreeNode extends SourceTreeNode {
    private MaxGraphSource graphSource;

    public GraphSourceTreeNode(GraphSchema schema, MaxGraphSource graphSource) {
        super(null, schema);
        this.graphSource = graphSource;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalQueryPlan.setDelegateSourceFlag(false);

        if (graphSource instanceof OdpsGraph) {
            OdpsGraph odpsGraph = OdpsGraph.class.cast(graphSource);
            QueryFlowOuterClass.OdpsGraphInput.Builder odpsGraphBuilder = QueryFlowOuterClass.OdpsGraphInput.newBuilder()
                    .setAccessId(odpsGraph.getAccessId())
                    .setAccessKey(odpsGraph.getAccessKey())
                    .setEndpoint(odpsGraph.getEndpoint())
                    .addAllEdgeInput(odpsGraph.getEdgeBuilderList()
                            .stream()
                            .map(Message.EdgeInput.Builder::build)
                            .collect(Collectors.toList()));
            ProcessorSourceFunction processorSourceFunction = new ProcessorSourceFunction(
                    QueryFlowOuterClass.OperatorType.GRAPH_SOURCE,
                    Message.Value.newBuilder()
                            .setPayload(odpsGraphBuilder.build().toByteString()),
                    null);
            return processSourceVertex(contextManager.getVertexIdManager(),
                    contextManager.getTreeNodeLabelManager(),
                    logicalQueryPlan,
                    processorSourceFunction);
        } else {
            throw new IllegalArgumentException("Only support odps graph yet.");
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return new VarietyValueType(Sets.newHashSet(new VertexValueType(), new EdgeValueType()));
    }
}
