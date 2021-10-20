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

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.google.common.collect.Lists;

import java.util.List;

public class AndTreeNode extends UnaryTreeNode {
    private List<TreeNode> andTreeNodeList = Lists.newArrayList();

    public AndTreeNode(TreeNode prev, GraphSchema schema) {
        super(prev, NodeType.FILTER, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        LogicalVertex filterVertex = delegateSourceVertex;
        for (TreeNode andTreeNode : andTreeNodeList) {
            LogicalQueryPlan logicalQueryPlan = new LogicalQueryPlan(contextManager);
            logicalQueryPlan.addLogicalVertex(filterVertex);
            filterVertex =
                    TreeNodeUtils.buildFilterTreeNode(
                            andTreeNode, contextManager, logicalQueryPlan, filterVertex, schema);
            logicalSubQueryPlan.mergeLogicalQueryPlan(logicalQueryPlan);
        }

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        addUsedLabelAndRequirement(outputVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(outputVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    public List<TreeNode> getAndTreeNodeList() {
        return andTreeNodeList;
    }
}
