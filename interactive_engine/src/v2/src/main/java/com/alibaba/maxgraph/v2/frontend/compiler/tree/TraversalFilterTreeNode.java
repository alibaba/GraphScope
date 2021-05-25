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

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;

public class TraversalFilterTreeNode extends UnaryTreeNode {
    private TreeNode filterTreeNode;

    public TraversalFilterTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.FILTER, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        if (filterTreeNode instanceof SourceTreeNode) {
            throw new IllegalArgumentException();
        } else {
            LogicalVertex sourceVertex = getInputNode().getOutputVertex();
            LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            logicalSubQueryPlan.addLogicalVertex(sourceVertex);
            LogicalVertex outputVertex = TreeNodeUtils.buildFilterTreeNode(this.filterTreeNode, contextManager, logicalSubQueryPlan, sourceVertex, schema);

            addUsedLabelAndRequirement(outputVertex, labelManager);
            setFinishVertex(outputVertex, labelManager);
            return logicalSubQueryPlan;
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    void setFilterTreeNode(TreeNode filterTreeNode) {
        this.filterTreeNode = filterTreeNode;
    }

}
