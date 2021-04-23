package com.alibaba.maxgraph.v2.frontend.compiler.tree.addition;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public interface CountFlagNode {
    boolean checkCountOptimize();

    void enableCountFlag();

    boolean isCountFlag();

    default OperatorType getCountOperator(OperatorType operatorType) {
        if (isCountFlag()) {
            return OperatorType.valueOf(operatorType + "_COUNT");
        } else {
            return operatorType;
        }
    }

    default ValueType getCountOutputType(ValueType valueType) {
        if (isCountFlag()) {
            return new ValueValueType(VariantType.VT_LONG);
        } else {
            return valueType;
        }
    }
}
