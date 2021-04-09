package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class UnfoldTreeNode extends UnaryTreeNode {
    public UnfoldTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.FLATMAP, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.UNFOLD, rangeLimit);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        ValueType inputValueType = getInputNode().getOutputValueType();
        if (inputValueType instanceof ListValueType) {
            return ListValueType.class.cast(inputValueType).getListValue();
        } else if (inputValueType instanceof MapValueType) {
            MapValueType mapValueType = MapValueType.class.cast(inputValueType);
            return new MapEntryValueType(mapValueType.getKey(), mapValueType.getValue());
        } else {
            return inputValueType;
        }
    }
}
