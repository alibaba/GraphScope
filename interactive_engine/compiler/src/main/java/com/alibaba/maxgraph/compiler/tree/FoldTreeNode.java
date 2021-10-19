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
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;

public class FoldTreeNode extends AbstractUseKeyNode {

    public FoldTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        QueryFlowOuterClass.OperatorType foldType =
                getUseKeyOperator(QueryFlowOuterClass.OperatorType.FOLD);
        LogicalVertex foldVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(foldType, Message.Value.newBuilder()),
                        false,
                        sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(foldVertex);
        logicalSubQueryPlan.addLogicalEdge(
                sourceVertex, foldVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));

        setFinishVertex(foldVertex, contextManager.getTreeNodeLabelManager());
        addUsedLabelAndRequirement(foldVertex, contextManager.getTreeNodeLabelManager());
        return logicalSubQueryPlan;
    }

    @Override
    public boolean isPropLocalFlag() {
        return false;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ListValueType(getInputNode().getOutputValueType());
    }
}
