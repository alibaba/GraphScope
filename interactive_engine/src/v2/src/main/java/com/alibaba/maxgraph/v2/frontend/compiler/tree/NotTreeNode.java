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
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;

public class NotTreeNode extends UnaryTreeNode {
    private TreeNode notTreeNode;

    public NotTreeNode(TreeNode input, GraphSchema schema, TreeNode notTreeNode) {
        super(input, NodeType.FILTER, schema);
        this.notTreeNode = notTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        TreeNode currentNotNode = TreeNodeUtils.buildSingleOutputNode(notTreeNode, schema);
        LogicalSubQueryPlan notQueryPlan = TreeNodeUtils.buildSubQueryPlanWithKey(currentNotNode, delegateSourceVertex, contextManager.getTreeNodeLabelManager(), contextManager, contextManager.getVertexIdManager());
        LogicalVertex notValueVertex = notQueryPlan.getOutputVertex();
        logicalSubQueryPlan.mergeLogicalQueryPlan(notQueryPlan);

        TreeNode sourceNode = TreeNodeUtils.getSourceTreeNode(currentNotNode);
        LogicalVertex enterKeyVertex = sourceNode.getOutputVertex();
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.JOIN_DIRECT_FILTER_NEGATE);
        LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(
                contextManager.getVertexIdManager().getId(),
                processorFunction,
                false,
                enterKeyVertex,
                notValueVertex);
        logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
        logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, logicalBinaryVertex, new LogicalEdge());
        logicalSubQueryPlan.addLogicalEdge(notValueVertex, logicalBinaryVertex, new LogicalEdge());

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        addUsedLabelAndRequirement(outputVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(outputVertex, contextManager.getTreeNodeLabelManager());
        return logicalSubQueryPlan;
    }

    public TreeNode getNotTreeNode() {
        return notTreeNode;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}
