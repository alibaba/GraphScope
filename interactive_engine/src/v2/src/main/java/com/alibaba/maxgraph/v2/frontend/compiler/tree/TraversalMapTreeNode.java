package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;

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
