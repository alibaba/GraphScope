package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;
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
            filterVertex = TreeNodeUtils.buildFilterTreeNode(andTreeNode, contextManager, logicalQueryPlan, filterVertex, schema);
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
