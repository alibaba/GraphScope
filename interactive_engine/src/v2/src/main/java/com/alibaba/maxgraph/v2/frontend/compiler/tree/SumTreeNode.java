package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public class SumTreeNode extends AbstractUseKeyNode implements JoinZeroNode {
    private boolean joinZeroFlag = false;

    public SumTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        OperatorType operatorType = getUseKeyOperator(OperatorType.SUM);
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ValueValueType valueValueType = ValueValueType.class.cast(getInputNode().getOutputValueType());
        ProcessorFunction combinerSumFunction = new ProcessorFunction(OperatorType.COMBINER_SUM,
                Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalVertex combinerSumVertex = new LogicalUnaryVertex(vertexIdManager.getId(), combinerSumFunction, isPropLocalFlag(), sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(combinerSumVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, combinerSumVertex, LogicalEdge.forwardEdge());

        ProcessorFunction sumFunction = new ProcessorFunction(operatorType, Value.newBuilder().setValueType(valueValueType.getDataType()));
        LogicalVertex sumVertex = new LogicalUnaryVertex(vertexIdManager.getId(), sumFunction, isPropLocalFlag(), combinerSumVertex);
        logicalSubQueryPlan.addLogicalVertex(sumVertex);
        logicalSubQueryPlan.addLogicalEdge(combinerSumVertex, sumVertex, new LogicalEdge());
        LogicalVertex outputVertex = processJoinZeroVertex(vertexIdManager, logicalSubQueryPlan, sumVertex, isJoinZeroFlag());

        setFinishVertex(outputVertex, labelManager);
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

    @Override
    public void disableJoinZero() {
        this.joinZeroFlag = false;
    }

    @Override
    public void enableJoinZero() {
        this.joinZeroFlag = true;
    }

    @Override
    public boolean isJoinZeroFlag() {
        return this.joinZeroFlag;
    }
}
