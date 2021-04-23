package com.alibaba.maxgraph.v2.frontend.compiler.tree;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.PropKeyValueType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.PropertyValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public class PropertyKeyValueTreeNode extends UnaryTreeNode {
    private PropKeyValueType propKeyValueType;

    public PropertyKeyValueTreeNode(TreeNode prev, GraphSchema schema, PropKeyValueType propKeyValueType) {
        super(prev, NodeType.MAP, schema);
        this.propKeyValueType = propKeyValueType;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Value.Builder argumentBuilder = Value.newBuilder()
                .setIntValue(propKeyValueType.getNumber());
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.PROP_KEY_VALUE, argumentBuilder);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        switch (propKeyValueType) {
            case PROP_KEY_TYPE: {
                return new ValueValueType(VariantType.VT_STRING);
            }
            default: {
                PropertyValueType propertyValueType = PropertyValueType.class.cast(getInputNode().getOutputValueType());
                return new ValueValueType(propertyValueType.getPropValueType());
            }
        }
    }
}
