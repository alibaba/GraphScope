package com.alibaba.graphscope.common.ir.rex;

import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rex.*;

import java.util.ArrayList;
import java.util.List;

/**
 * convert each variable alias to the {@code targetAliasId} in an expression
 */
public class RexVariableAliasConverter extends RexVisitorImpl<RexNode> {
    private final GraphBuilder builder;
    private final String targetAliasName;
    private final int targetAliasId;

    public RexVariableAliasConverter(
            boolean deep, GraphBuilder builder, String targetAliasName, int targetAliasId) {
        super(deep);
        this.builder = builder;
        this.targetAliasName = targetAliasName;
        this.targetAliasId = targetAliasId;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (this.deep) {
            List<RexNode> results = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                results.add(operand.accept(this));
            }
            return builder.call(call.getOperator(), results);
        }
        return null;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return (inputRef instanceof RexGraphVariable)
                ? visitGraphVariable((RexGraphVariable) inputRef)
                : inputRef;
    }

    public RexNode visitGraphVariable(RexGraphVariable variable) {
        int delimPos = variable.getName().indexOf(AliasInference.DELIMITER);
        String targetVarName =
                (delimPos < 0)
                        ? targetAliasName
                        : targetAliasName
                                + AliasInference.DELIMITER
                                + variable.getName().substring(delimPos + 1);
        return RexGraphVariable.of(
                targetAliasId, variable.getProperty(), targetVarName, variable.getType());
    }
}
