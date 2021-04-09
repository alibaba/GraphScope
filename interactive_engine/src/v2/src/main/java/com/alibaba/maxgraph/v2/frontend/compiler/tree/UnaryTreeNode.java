package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class UnaryTreeNode extends BaseTreeNode {
    private TreeNode input;

    public UnaryTreeNode(TreeNode input, NodeType nodeType, GraphSchema schema) {
        super(nodeType, schema);
        this.input = input;
        if (null != this.input) {
            this.input.setOutputNode(this);
        }
    }

    public TreeNode getInputNode() {
        return input;
    }

    public void setInputNode(TreeNode treeNode) {
        this.input = checkNotNull(treeNode);
        this.input.setOutputNode(this);
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(VertexIdManager vertexIdManager,
                                                         TreeNodeLabelManager labelManager,
                                                         ProcessorFunction processorFunction,
                                                         ContextManager contextManager) {
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager, new LogicalEdge());
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(VertexIdManager vertexIdManager,
                                                         TreeNodeLabelManager labelManager,
                                                         ProcessorFunction processorFunction,
                                                         ContextManager contextManager,
                                                         LogicalEdge logicalEdge) {
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager, logicalEdge, true);
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(VertexIdManager vertexIdManager,
                                                         TreeNodeLabelManager labelManager,
                                                         ProcessorFunction processorFunction,
                                                         ContextManager contextManager,
                                                         LogicalEdge logicalEdge,
                                                         boolean outputFlag) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        LogicalUnaryVertex logicalUnaryVertex = new LogicalUnaryVertex(
                vertexIdManager.getId(),
                processorFunction,
                false,
                sourceVertex);
        logicalUnaryVertex.setEarlyStopFlag(super.earlyStopArgument);
        logicalSubQueryPlan.addLogicalVertex(logicalUnaryVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, logicalUnaryVertex, logicalEdge);

        if (outputFlag) {
            setFinishVertex(logicalUnaryVertex, labelManager);
            addUsedLabelAndRequirement(logicalUnaryVertex, labelManager);
        }
        return logicalSubQueryPlan;
    }

    protected void addUsedLabelAndRequirement(LogicalVertex logicalVertex, TreeNodeLabelManager treeNodeLabelManager) {
        getUsedLabelList().forEach(v -> logicalVertex.getProcessorFunction().getUsedLabelList().add(treeNodeLabelManager.getLabelIndex(v)));
        logicalVertex.getBeforeRequirementList().addAll(buildBeforeRequirementList(treeNodeLabelManager));
        logicalVertex.getAfterRequirementList().addAll(buildAfterRequirementList(treeNodeLabelManager));
    }
}
