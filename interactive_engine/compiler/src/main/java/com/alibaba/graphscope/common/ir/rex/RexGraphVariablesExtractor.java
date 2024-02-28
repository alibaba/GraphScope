package com.alibaba.graphscope.common.ir.rex;

import com.google.common.base.Preconditions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * extract all graph variables in an expression
 */
public class RexGraphVariablesExtractor extends RexVisitorImpl<List<RexGraphVariable>> {
    private List<RexGraphVariable> extractedGraphVariables;

    public RexGraphVariablesExtractor(boolean deep) {
        super(deep);
        this.extractedGraphVariables = new ArrayList<>();
    }

    @Override
    public List<RexGraphVariable> visitCall(RexCall call) {
        if (!this.deep) {
            return null;
        }
        SqlOperator operator = call.getOperator();
        if (operator.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
            return visitArrayValueConstructor(call);
        } else if (operator.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR) {
            return visitMapValueConstructor(call);
        } else {
            return super.visitCall(call);
        }
    }

    private List<RexGraphVariable> visitArrayValueConstructor(RexCall call) {
        call.getOperands()
                .forEach(
                        operand -> {
                            Preconditions.checkArgument(
                                    operand instanceof RexGraphVariable,
                                    "component type of 'ARRAY_VALUE_CONSTRUCTOR' should be"
                                            + " 'variable' in ir core structure");
                            extractedGraphVariables.add((RexGraphVariable) operand);
                        });
        return extractedGraphVariables;
    }

    private List<RexGraphVariable> visitMapValueConstructor(RexCall call) {
        List<RexNode> operands = call.getOperands();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            RexNode value = operands.get(i + 1);
            Preconditions.checkArgument(
                    value instanceof RexGraphVariable,
                    "value type of 'MAP_VALUE_CONSTRUCTOR' should be 'variable', but is "
                            + value.getClass());
            extractedGraphVariables.add((RexGraphVariable) value);
        }
        return extractedGraphVariables;
    }

    @Override
    public List<RexGraphVariable> visitInputRef(RexInputRef inputRef) {
        if (inputRef instanceof RexGraphVariable) {
            extractedGraphVariables.add((RexGraphVariable) inputRef);
        }
        return extractedGraphVariables;
    }
}
