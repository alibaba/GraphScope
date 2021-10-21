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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.tinkerpop.steps.ShortestPathStep;

import static com.google.common.base.Preconditions.checkArgument;

public class ShortestPathTreeNode extends UnaryTreeNode {
    private final ShortestPathStep step;

    public ShortestPathTreeNode(TreeNode input, GraphSchema schema, ShortestPathStep step) {
        super(input, NodeType.FLATMAP, schema);
        this.step = step;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_SHORTESTPATH,
                createOperatorArgument());
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramShortestPathArg.Builder shortestPathArgBuilder = Message.ProgramShortestPathArg.newBuilder();
        shortestPathArgBuilder.setSid(step.sid);
        shortestPathArgBuilder.setTid(step.tid);
        shortestPathArgBuilder.setPropertyIdId(schema.getPropertyId(step.sidPropId));
        if (step.edgeWeightPropId != null) {
            shortestPathArgBuilder.setPropertyEdgeWeightId(schema.getPropertyId(step.edgeWeightPropId));
        } else {
            shortestPathArgBuilder.setPropertyEdgeWeightId(-1);
        }
        shortestPathArgBuilder.setPropertyPathId(schema.getPropertyId(step.outPropId));
        checkArgument(step.iteration > 0, "iteration must > 0 for shortest path");
        shortestPathArgBuilder.setLoopLimit(step.iteration);
        valueBuilder.setPayload(shortestPathArgBuilder.build().toByteString());

        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return null;
    }
}
