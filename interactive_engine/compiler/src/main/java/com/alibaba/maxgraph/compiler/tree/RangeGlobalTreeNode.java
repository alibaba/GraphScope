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
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;

public class RangeGlobalTreeNode extends AbstractUseKeyNode {
    private long low;
    private long high;

    public RangeGlobalTreeNode(TreeNode prev, GraphSchema schema, long low, long high) {
        super(prev, NodeType.FILTER, schema);
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ProcessorFunction combinerFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.COMBINER_RANGE,
                createArgumentBuilder().addLongValueList(0)
                        .addLongValueList(high));
        LogicalUnaryVertex combinerVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
                combinerFunction,
                false,
                sourceVertex);
        combinerVertex.setEarlyStopFlag(super.earlyStopArgument);
        logicalSubQueryPlan.addLogicalVertex(combinerVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, combinerVertex, LogicalEdge.forwardEdge());

        QueryFlowOuterClass.OperatorType operatorType = getUseKeyOperator(QueryFlowOuterClass.OperatorType.RANGE);
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                .addLongValueList(low)
                .addLongValueList(high);
        ProcessorFunction processorFunction = new ProcessorFunction(operatorType, argumentBuilder);
        LogicalUnaryVertex rangeVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, combinerVertex);

        logicalSubQueryPlan.addLogicalVertex(rangeVertex);
        logicalSubQueryPlan.addLogicalEdge(combinerVertex, rangeVertex, new LogicalEdge());

        addUsedLabelAndRequirement(rangeVertex, labelManager);
        setFinishVertex(rangeVertex, labelManager);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}
