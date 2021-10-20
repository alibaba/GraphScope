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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.PropertyValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;

public class PropertyKeyValueTreeNode extends UnaryTreeNode {
    private Message.PropKeyValueType propKeyValueType;

    public PropertyKeyValueTreeNode(TreeNode prev, GraphSchema schema, Message.PropKeyValueType propKeyValueType) {
        super(prev, NodeType.MAP, schema);
        this.propKeyValueType = propKeyValueType;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                .setIntValue(propKeyValueType.getNumber());
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.PROP_KEY_VALUE, argumentBuilder);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        switch (propKeyValueType) {
            case PROP_KEY_TYPE: {
                return new ValueValueType(Message.VariantType.VT_STRING);
            }
            default: {
                PropertyValueType propertyValueType = PropertyValueType.class.cast(getInputNode().getOutputValueType());
                return new ValueValueType(propertyValueType.getPropValueType());
            }
        }
    }
}
