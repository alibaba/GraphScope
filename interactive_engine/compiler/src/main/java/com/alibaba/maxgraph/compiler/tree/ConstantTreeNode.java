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
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConstantTreeNode extends UnaryTreeNode {
    private Object constant;
    private Message.VariantType variantType;

    public ConstantTreeNode(TreeNode prev, GraphSchema schema, Object constant) {
        super(prev, NodeType.MAP, schema);
        this.constant = checkNotNull(constant);
        this.variantType = CompilerUtils.parseVariantType(constant.getClass(), constant);
    }

    public Object getConstant() {
        return constant;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                .setValueType(variantType);
        switch (variantType) {
            case VT_INT: {
                argumentBuilder.setIntValue((int) constant);
                break;
            }
            case VT_LONG: {
                argumentBuilder.setLongValue((long) constant);
                break;
            }
            case VT_FLOAT: {
                argumentBuilder.setFloatValue((float) constant);
                break;
            }
            case VT_DOUBLE: {
                argumentBuilder.setDoubleValue((double) constant);
                break;
            }
            case VT_STRING: {
                argumentBuilder.setStrValue((String) constant);
                break;
            }
            case VT_INT_LIST: {
                argumentBuilder.addAllIntValueList((Collection<Integer>) constant);
                break;
            }
            case VT_LONG_LIST: {
                argumentBuilder.addAllLongValueList((Collection<Long>) constant);
                break;
            }
            case VT_STRING_LIST: {
                argumentBuilder.addAllStrValueList((Collection<String>) constant);
                break;
            }
            default: {
                throw new IllegalArgumentException("Not support " + variantType + " constant value");
            }

        }
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.CONSTANT, argumentBuilder);
        return parseSingleUnaryVertex(contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(variantType);
    }
}
