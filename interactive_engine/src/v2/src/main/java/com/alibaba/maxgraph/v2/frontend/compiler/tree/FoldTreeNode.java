package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class FoldTreeNode extends AbstractUseKeyNode {

    public FoldTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        OperatorType foldType = getUseKeyOperator(OperatorType.FOLD);
        LogicalVertex foldVertex = new LogicalUnaryVertex(
                contextManager.getVertexIdManager().getId(),
                new ProcessorFunction(foldType, Value.newBuilder()),
                false,
                sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(foldVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, foldVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));

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
