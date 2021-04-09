package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VertexValueType;

public class EdgeOtherVertexTreeNode extends UnaryTreeNode {
    public EdgeOtherVertexTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.OTHER_V);
        return parseSingleUnaryVertex(contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}
