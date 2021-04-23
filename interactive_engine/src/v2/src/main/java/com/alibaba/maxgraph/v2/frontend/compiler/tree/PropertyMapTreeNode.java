package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.PropertyType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.PropertyValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;

public class PropertyMapTreeNode extends UnaryTreeNode implements PropertyNode {
    private String[] propertyKeys;
    private org.apache.tinkerpop.gremlin.structure.PropertyType propertyType;
    private boolean includeTokens;

    public PropertyMapTreeNode(
            TreeNode prev,
            GraphSchema schema,
            String[] propertyKeys,
            org.apache.tinkerpop.gremlin.structure.PropertyType propertyType,
            boolean includeTokens) {
        super(prev, NodeType.MAP, schema);
        this.propertyKeys = propertyKeys;
        this.propertyType = propertyType;
        this.includeTokens = includeTokens;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Value.Builder argumentBuilder = Value.newBuilder()
                .setBoolValue(includeTokens)
                .setIntValue(propertyType == org.apache.tinkerpop.gremlin.structure.PropertyType.PROPERTY ? PropertyType.PROP_TYPE_VALUE : PropertyType.VALUE_TYPE_VALUE)
                .setBoolFlag(edgePropFlag());
        if (null != propertyKeys) {
            Arrays.stream(propertyKeys).forEach(v -> argumentBuilder.addIntValueList(SchemaUtils.getPropId(v, schema)));
        }
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.PROP_MAP_VALUE, argumentBuilder);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        Set<VariantType> variantTypeList = Sets.newHashSet();
        if (null != propertyKeys) {
            for (String propKey : propertyKeys) {
                SchemaUtils.getDataTypeList(propKey, schema)
                        .forEach(v -> variantTypeList.add(CompilerUtils.parseVariantFromDataType(v)));
            }
        }
        if (null == propertyKeys) {
            if (propertyType == org.apache.tinkerpop.gremlin.structure.PropertyType.VALUE) {
                return new MapValueType(new ValueValueType(VariantType.VT_STRING), new ListValueType(new ValueValueType(VariantType.VT_UNKNOWN)));
            } else {
                return new MapValueType(new ValueValueType(VariantType.VT_STRING), new ListValueType(new PropertyValueType(VariantType.VT_UNKNOWN)));
            }
        } else {
            if (propertyType == org.apache.tinkerpop.gremlin.structure.PropertyType.VALUE) {
                return new MapValueType(new ValueValueType(VariantType.VT_STRING), new ListValueType(new ValueValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? VariantType.VT_UNKNOWN : variantTypeList.iterator().next())));
            } else {
                return new MapValueType(new ValueValueType(VariantType.VT_STRING), new ListValueType(new PropertyValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? VariantType.VT_UNKNOWN : variantTypeList.iterator().next())));
            }
        }
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public Set<String> getPropKeyList() {
        return Sets.newHashSet(propertyKeys);
    }

    @Override
    public boolean edgePropFlag() {
        return getInputNode().getOutputValueType() instanceof EdgeValueType;
    }
}
