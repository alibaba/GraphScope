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
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class RangeGlobalTreeNode extends AbstractUseKeyNode {
    private long low;
    private long high;

    public RangeGlobalTreeNode(TreeNode prev, GraphSchema schema, long low, long high) {
        super(prev, NodeType.FILTER, schema);
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ProcessorFunction combinerFunction = new ProcessorFunction(OperatorType.COMBINER_RANGE,
                createArgumentBuilder().addLongValueList(0)
                        .addLongValueList(high));
        LogicalUnaryVertex combinerVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
                combinerFunction,
                false,
                sourceVertex);
        combinerVertex.setEarlyStopFlag(super.earlyStopArgument);
        logicalSubQueryPlan.addLogicalVertex(combinerVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, combinerVertex, LogicalEdge.forwardEdge());

        OperatorType operatorType = getUseKeyOperator(OperatorType.RANGE);
        Value.Builder argumentBuilder = Value.newBuilder()
                .addLongValueList(low)
                .addLongValueList(high);
        ProcessorFunction processorFunction = new ProcessorFunction(operatorType, argumentBuilder);
        LogicalUnaryVertex rangeVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, combinerVertex);

        logicalSubQueryPlan.addLogicalVertex(rangeVertex);
        logicalSubQueryPlan.addLogicalEdge(combinerVertex, rangeVertex, new LogicalEdge());

        addUsedLabelAndRequirement(rangeVertex, labelManager);
        setFinishVertex(rangeVertex, labelManager);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}
