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
import com.alibaba.maxgraph.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import org.apache.tinkerpop.gremlin.structure.T;

public class TokenTreeNode extends UnaryTreeNode {
    private T token;

    public TokenTreeNode(TreeNode input, GraphSchema schema, T token) {
        super(input, NodeType.MAP, schema);
        this.token = token;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder();
        switch (token) {
            case id: {
                argumentBuilder.addIntValueList(TreeConstants.ID_INDEX);
                break;
            }
            case label: {
                argumentBuilder.addIntValueList(TreeConstants.LABEL_INDEX);
                break;
            }
            case key: {
                argumentBuilder.addIntValueList(TreeConstants.KEY_INDEX);
                break;
            }
            case value: {
                argumentBuilder.addIntValueList(TreeConstants.VALUE_INDEX);
                break;
            }
        }
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.PROP_VALUE, argumentBuilder);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        switch (token) {
            case id:
                return new ValueValueType(Message.VariantType.VT_LONG);
            case label:
                return new ValueValueType(Message.VariantType.VT_STRING);
            case key:
                return MapEntryValueType.class.cast(getInputNode().getOutputValueType()).getKey();
            case value:
                return MapEntryValueType.class.cast(getInputNode().getOutputValueType()).getValue();
            default:
                throw new IllegalArgumentException();
        }
    }

    public T getToken() {
        return token;
    }
}
