package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public class MaxTreeNode extends AbstractUseKeyNode {

    public MaxTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ValueValueType valueValueType = ValueValueType.class.cast(getInputNode().getOutputValueType());
        OperatorType operatorType = getUseKeyOperator(OperatorType.MAX);
        ProcessorFunction maxFunction = new ProcessorFunction(operatorType, Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalUnaryVertex maxVertex = new LogicalUnaryVertex(
                contextManager.getVertexIdManager().getId(),
                maxFunction,
                isPropLocalFlag(),
                sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(maxVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, maxVertex, new LogicalEdge());

        setFinishVertex(maxVertex, contextManager.getTreeNodeLabelManager());
        addUsedLabelAndRequirement(maxVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public boolean isPropLocalFlag() {
        return false;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}
