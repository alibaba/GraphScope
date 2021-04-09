package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VertexValueType;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class EdgeVertexTreeNode extends UnaryTreeNode {
    private Direction direction;

    public EdgeVertexTreeNode(TreeNode input, Direction direction, GraphSchema schema) {
        super(input, direction == Direction.BOTH ? NodeType.FLATMAP : NodeType.MAP, schema);
        this.direction = direction;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(
            ContextManager contextManager) {
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.valueOf(direction.name() + "_V"), rangeLimit);

        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }

    public Direction getDirection() {
        return direction;
    }
}
