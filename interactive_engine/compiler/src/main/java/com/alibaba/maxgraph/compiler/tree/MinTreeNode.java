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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;

public class MinTreeNode extends AbstractUseKeyNode {

    public MinTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ValueValueType valueValueType =
                ValueValueType.class.cast(getInputNode().getOutputValueType());
        QueryFlowOuterClass.OperatorType operatorType =
                getUseKeyOperator(QueryFlowOuterClass.OperatorType.MIN);
        ProcessorFunction minFunction =
                new ProcessorFunction(
                        operatorType,
                        Message.Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalUnaryVertex minVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        minFunction,
                        isPropLocalFlag(),
                        sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(minVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, minVertex, new LogicalEdge());

        setFinishVertex(minVertex, contextManager.getTreeNodeLabelManager());
        addUsedLabelAndRequirement(minVertex, contextManager.getTreeNodeLabelManager());

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
}
