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
package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public class SumTreeNode extends AbstractUseKeyNode implements JoinZeroNode {
    private boolean joinZeroFlag = false;

    public SumTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        OperatorType operatorType = getUseKeyOperator(OperatorType.SUM);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ValueValueType valueValueType = ValueValueType.class.cast(getInputNode().getOutputValueType());
        ProcessorFunction combinerSumFunction = new ProcessorFunction(OperatorType.COMBINER_SUM,
                Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalVertex combinerSumVertex = new LogicalUnaryVertex(vertexIdManager.getId(), combinerSumFunction, isPropLocalFlag(), sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(combinerSumVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, combinerSumVertex, LogicalEdge.forwardEdge());

        ProcessorFunction sumFunction = new ProcessorFunction(operatorType, Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalVertex sumVertex = new LogicalUnaryVertex(vertexIdManager.getId(), sumFunction, isPropLocalFlag(), combinerSumVertex);
        logicalSubQueryPlan.addLogicalVertex(sumVertex);
        logicalSubQueryPlan.addLogicalEdge(combinerSumVertex, sumVertex, new LogicalEdge());
        LogicalVertex outputVertex = processJoinZeroVertex(vertexIdManager, logicalSubQueryPlan, sumVertex, isJoinZeroFlag());

        setFinishVertex(outputVertex, labelManager);
        return logicalSubQueryPlan;
    }

    @Override
    public boolean isPropLocalFlag() {
        return false;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    @Override
    public void disableJoinZero() {
        this.joinZeroFlag = false;
    }

    @Override
    public void enableJoinZero() {
        this.joinZeroFlag = true;
    }

    @Override
    public boolean isJoinZeroFlag() {
        return this.joinZeroFlag;
    }
}
