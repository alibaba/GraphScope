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

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;

public class TraversalMapTreeNode extends UnaryTreeNode {
    private TreeNode traversalNode;

    public TraversalMapTreeNode(TreeNode prev, GraphSchema schema, TreeNode traversalTreeNode) {
        super(prev, NodeType.MAP, schema);
        this.traversalNode = traversalTreeNode;
    }

    public TreeNode getTraversalNode() {
        return traversalNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        TreeNode currTraversalNode = TreeNodeUtils.buildSingleOutputNode(traversalNode, schema);
        LogicalSubQueryPlan traversalPlan = TreeNodeUtils.buildQueryPlanWithSource(currTraversalNode, labelManager, contextManager, vertexIdManager, delegateSourceVertex);
        logicalSubQueryPlan.mergeLogicalQueryPlan(traversalPlan);

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        addUsedLabelAndRequirement(outputVertex, labelManager);
        setFinishVertex(outputVertex, labelManager);
        
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return traversalNode.getOutputValueType();
    }
}
