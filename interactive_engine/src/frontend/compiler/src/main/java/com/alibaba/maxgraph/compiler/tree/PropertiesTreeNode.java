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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.compiler.tree.value.PropertyValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
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
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder().setBoolFlag(edgePropFlag());
        if (null != propertyKeys) {
            Arrays.stream(propertyKeys).forEach(v -> argumentBuilder.addIntValueList(CompilerUtils.getPropertyId(schema, v)));
        }
        QueryFlowOuterClass.OperatorType operatorType = returnType == PropertyType.VALUE ? QueryFlowOuterClass.OperatorType.PROP_VALUE : QueryFlowOuterClass.OperatorType.PROPERTIES;
        ProcessorFunction processorFunction = new ProcessorFunction(operatorType, argumentBuilder);

        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        Set<Message.VariantType> variantTypeList = Sets.newHashSet();
        if (null != propertyKeys) {
            for (String propKey : propertyKeys) {
                Set<DataType> dataTypeList = SchemaUtils.getPropDataTypeList(propKey, schema);
                dataTypeList.forEach(v -> variantTypeList.add(CompilerUtils.parseVariantFromDataType(v)));
            }
        }
        if (null == propertyKeys) {
            if (returnType == PropertyType.VALUE) {
                return new ValueValueType(Message.VariantType.VT_UNKNOWN);
            } else {
                return new PropertyValueType(Message.VariantType.VT_UNKNOWN);
            }
        } else {
            if (returnType == PropertyType.VALUE) {
                return new ValueValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? Message.VariantType.VT_UNKNOWN : variantTypeList.iterator().next());
            } else {
                return new PropertyValueType(variantTypeList.isEmpty() || variantTypeList.size() > 1 ? Message.VariantType.VT_UNKNOWN : variantTypeList.iterator().next());
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
