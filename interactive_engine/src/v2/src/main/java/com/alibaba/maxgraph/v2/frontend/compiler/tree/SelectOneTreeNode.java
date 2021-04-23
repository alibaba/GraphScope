package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.List;
import java.util.Map;

public class SelectOneTreeNode extends UnaryTreeNode {
    private String selectLabel;
    private Pop pop;
    private TreeNode traversalTreeNode;
    private List<TreeNode> selectTreeNodeList;
    private ValueType constantValueType;

    public SelectOneTreeNode(TreeNode input, String selectLabel, Pop pop, List<TreeNode> selectTreeNodeList, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
        this.selectLabel = selectLabel;
        this.pop = pop;
        this.traversalTreeNode = null;
        this.selectTreeNodeList = selectTreeNodeList;
        this.constantValueType = null;
        getUsedLabelList().add(selectLabel);
    }

    public void setConstantValueType(ValueType constantValueType) {
        this.constantValueType = constantValueType;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Map<String, Integer> labelIndexList = labelManager.getLabelIndexList();

        ProcessorFunction selectOneFunction = TreeNodeUtils.createSelectOneFunction(selectLabel, pop, labelIndexList);
        LogicalSubQueryPlan logicalSubQueryPlan = parseSingleUnaryVertex(vertexIdManager,
                labelManager,
                selectOneFunction,
                contextManager,
                LogicalEdge.shuffleByKey(0),
                null == traversalTreeNode);

        if (null != traversalTreeNode &&
                !contextManager.getCostModelManager().processFieldValue(selectLabel)) {
            LogicalSubQueryPlan traversalValuePlan = TreeNodeUtils.buildSubQueryPlan(
                    traversalTreeNode,
                    logicalSubQueryPlan.getOutputVertex(),
                    contextManager);
            logicalSubQueryPlan.mergeLogicalQueryPlan(traversalValuePlan);

            LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
            addUsedLabelAndRequirement(outputVertex, labelManager);
            setFinishVertex(outputVertex, labelManager);
        }
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return constantValueType != null ? constantValueType :
                (null != traversalTreeNode && !(traversalTreeNode instanceof SourceTreeNode) ?
                        traversalTreeNode.getOutputValueType() :
                        CompilerUtils.parseValueTypeWithPop(selectTreeNodeList, pop));
    }

    public void setTraversalTreeNode(TreeNode traversalTreeNode) {
        this.traversalTreeNode = traversalTreeNode;
    }

    public TreeNode getTraversalTreeNode() {
        return traversalTreeNode;
    }

    public String getSelectLabel() {
        return selectLabel;
    }

    public TreeNode getLabelStartTreeNode() {
        if (this.selectTreeNodeList.size() == 1) {
            return this.selectTreeNodeList.get(0);
        }

        return null;
    }
}
