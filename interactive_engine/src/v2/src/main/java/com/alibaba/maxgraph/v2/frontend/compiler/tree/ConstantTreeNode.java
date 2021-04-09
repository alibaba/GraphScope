package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.CompilerUtils;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConstantTreeNode extends UnaryTreeNode {
    private Object constant;
    private VariantType variantType;

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
        Value.Builder argumentBuilder = Value.newBuilder()
                .setValueType(variantType);
        switch (variantType) {
            case VT_INTEGER: {
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
            case VT_INTEGER_LIST: {
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
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.CONSTANT, argumentBuilder);
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
