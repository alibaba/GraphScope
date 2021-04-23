package com.alibaba.maxgraph.v2.frontend.compiler.tree;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.TreeNodeUtils;

public class StoreTreeNode extends UnaryTreeNode {
    private String sideEffectKey;
    private TreeNode storeTreeNode;

    public StoreTreeNode(TreeNode input, GraphSchema schema, String sideEffectKey, TreeNode storeTreeNode) {
        super(input, NodeType.STORE, schema);
        this.sideEffectKey = sideEffectKey;
        this.storeTreeNode = storeTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalVertex storeVertex;
        LogicalSubQueryPlan logicalSubQueryPlan;
        if (null == storeTreeNode) {
            ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.FOLD_STORE);
            logicalSubQueryPlan = parseSingleUnaryVertex(vertexIdManager, treeNodeLabelManager, processorFunction, contextManager);
            storeVertex = logicalSubQueryPlan.getOutputVertex();
        } else {
            logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            LogicalVertex sourceVertex = getInputNode().getOutputVertex();
            logicalSubQueryPlan.addLogicalVertex(sourceVertex);

            LogicalSubQueryPlan storeQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(storeTreeNode, treeNodeLabelManager, contextManager, vertexIdManager, sourceVertex);
            logicalSubQueryPlan.mergeLogicalQueryPlan(storeQueryPlan);

            LogicalVertex valueVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.FOLD_STORE);
            LogicalVertex foldVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, valueVertex);
            logicalSubQueryPlan.addLogicalVertex(foldVertex);
            logicalSubQueryPlan.addLogicalEdge(valueVertex, foldVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));
            storeVertex = logicalSubQueryPlan.getOutputVertex();
        }
        storeVertex.enableStoreFlag();
        setFinishVertex(getInputNode().getOutputVertex(), treeNodeLabelManager);
        contextManager.addStoreVertex(sideEffectKey, storeVertex);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        if (null == storeTreeNode) {
            return new ListValueType(this.getInputNode().getOutputValueType());
        } else {
            return new ListValueType(storeTreeNode.getOutputValueType());
        }
    }
}
