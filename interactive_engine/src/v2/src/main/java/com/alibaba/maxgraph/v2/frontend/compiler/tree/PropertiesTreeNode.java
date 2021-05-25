/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.PropertyValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

import java.util.Arrays;
import java.util.Set;

public class PropertiesTreeNode extends UnaryTreeNode implements PropertyNode {
    private String[] propertyKeys;
    private PropertyType returnType;

    public PropertiesTreeNode(TreeNode prev, GraphSchema schema, String[] propertyKeys, PropertyType returnType) {
        super(prev, null == propertyKeys || propertyKeys.length > 1 ? NodeType.FLATMAP : NodeType.MAP, schema);
        this.propertyKeys = propertyKeys;
        this.returnType = returnType;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Value.Builder argumentBuilder = Value.newBuilder().setBoolFlag(edgePropFlag());
        if (null != propertyKeys) {
            Arrays.stream(propertyKeys).forEach(v -> argumentBuilder.addIntValueList(CompilerUtils.getPropertyId(schema, v)));
        }
        OperatorType operatorType = returnType == PropertyType.VALUE ? OperatorType.PROP_VALUE : OperatorType.PROPERTIES;
        ProcessorFunction processorFunction = new ProcessorFunction(operatorType, argumentBuilder);

        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        Set<VariantType> variantTypeList = Sets.newHashSet();
        if (null != propertyKeys) {
            for (String propKey : propertyKeys) {
                Set<DataType> dataTypeList = SchemaUtils.getDataTypeList(propKey, schema);
                dataTypeList.forEach(v -> variantTypeList.add(CompilerUtils.parseVariantFromDataType(v)));
            }
        }
        if (null == propertyKeys) {
            if (returnType == PropertyType.VALUE) {
                return new ValueValueType(VariantType.VT_UNKNOWN);
            } else {
                return new PropertyValueType(VariantType.VT_UNKNOWN);
            }
        } else {
            if (returnType == PropertyType.VALUE) {
                return new ValueValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? VariantType.VT_UNKNOWN : variantTypeList.iterator().next());
            } else {
                return new PropertyValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? VariantType.VT_UNKNOWN : variantTypeList.iterator().next());
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
