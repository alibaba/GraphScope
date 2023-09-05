/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.runtime.proto;

import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.DataType;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;

import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * convert an expression in calcite to logical expression in ir_core
 */
public class RexToProtoConverter extends RexVisitorImpl<OuterExpression.Expression> {
    private final boolean isColumnId;

    public RexToProtoConverter(boolean deep, boolean isColumnId) {
        super(deep);
        this.isColumnId = isColumnId;
    }

    @Override
    public OuterExpression.Expression visitCall(RexCall call) {
        if (!this.deep) {
            return null;
        }
        SqlOperator operator = call.getOperator();
        if (operator.getKind() == SqlKind.CASE) {
            return visitCase(call);
        } else if (operator.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
            return visitArrayValueConstructor(call);
        } else if (call.getOperands().size() == 1) {
            return visitUnaryOperator(call);
        } else {
            return visitBinaryOperator(call);
        }
    }

    private OuterExpression.Expression visitCase(RexCall call) {
        OuterExpression.Case.Builder caseBuilder = OuterExpression.Case.newBuilder();
        int operandCount = call.getOperands().size();
        Preconditions.checkArgument(operandCount > 2 && (operandCount & 1) == 1);
        for (int i = 1; i < operandCount - 1; i += 2) {
            RexNode whenNode = call.getOperands().get(i - 1);
            RexNode thenNode = call.getOperands().get(i);
            caseBuilder.addWhenThenExpressions(
                    OuterExpression.Case.WhenThen.newBuilder()
                            .setWhenExpression(whenNode.accept(this))
                            .setThenResultExpression(thenNode.accept(this)));
        }
        caseBuilder.setElseResultExpression(call.getOperands().get(operandCount - 1).accept(this));
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setCase(caseBuilder)
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)))
                .build();
    }

    private OuterExpression.Expression visitArrayValueConstructor(RexCall call) {
        OuterExpression.VariableKeys.Builder varsBuilder =
                OuterExpression.VariableKeys.newBuilder();
        call.getOperands()
                .forEach(
                        operand -> {
                            Preconditions.checkArgument(
                                    operand instanceof RexGraphVariable,
                                    "component type of 'ARRAY_VALUE_CONSTRUCTOR' should be"
                                            + " 'variable' in ir core structure");
                            varsBuilder.addKeys(operand.accept(this).getOperators(0).getVar());
                        });
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setVars(varsBuilder)
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)))
                .build();
    }

    private OuterExpression.Expression visitUnaryOperator(RexCall call) {
        SqlOperator operator = call.getOperator();
        RexNode operand = call.getOperands().get(0);
        switch (operator.getKind()) {
                // convert IS_NULL to unary call: IS_NULL(XX)
            case IS_NULL:
                return visitIsNullOperator(operand);
                // convert IS_NOT_NULL to NOT(IS_NULL(XX))
            case IS_NOT_NULL:
                return OuterExpression.Expression.newBuilder()
                        .addOperators(Utils.protoOperator(GraphStdOperatorTable.NOT))
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE))
                        .addAllOperators(visitIsNullOperator(operand).getOperatorsList())
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE))
                        .build();
            case NOT:
            default:
                return OuterExpression.Expression.newBuilder()
                        .addOperators(Utils.protoOperator(operator))
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE))
                        .addAllOperators(operand.accept(this).getOperatorsList())
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE))
                        .build();
        }
    }

    private OuterExpression.Expression visitIsNullOperator(RexNode operand) {
        return OuterExpression.Expression.newBuilder()
                .addOperators(Utils.protoOperator(GraphStdOperatorTable.IS_NULL))
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE))
                .addAllOperators(operand.accept(this).getOperatorsList())
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE))
                .build();
    }

    private OuterExpression.Expression visitBinaryOperator(RexCall call) {
        SqlOperator operator = call.getOperator();
        OuterExpression.Expression.Builder exprBuilder = OuterExpression.Expression.newBuilder();
        // left-associative
        if (operator.getLeftPrec() <= operator.getRightPrec()) {
            for (int i = 0; i < call.getOperands().size(); ++i) {
                RexNode operand = call.getOperands().get(i);
                if (i != 0) {
                    exprBuilder.addOperators(
                            Utils.protoOperator(operator).toBuilder()
                                    .setNodeType(
                                            Utils.protoIrDataType(call.getType(), isColumnId)));
                }
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(
                            OuterExpression.ExprOpr.newBuilder()
                                    .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE)
                                    .build());
                }
                exprBuilder.addAllOperators(operand.accept(this).getOperatorsList());
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(
                            OuterExpression.ExprOpr.newBuilder()
                                    .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE)
                                    .build());
                }
            }
        } else { // right-associative
            for (int i = call.getOperands().size() - 1; i >= 0; --i) {
                RexNode operand = call.getOperands().get(i);
                if (i != 0) {
                    exprBuilder.addOperators(
                            Utils.protoOperator(operator).toBuilder()
                                    .setNodeType(
                                            Utils.protoIrDataType(call.getType(), isColumnId)));
                }
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(
                            OuterExpression.ExprOpr.newBuilder()
                                    .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE)
                                    .build());
                }
                exprBuilder.addAllOperators(operand.accept(this).getOperatorsList());
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(
                            OuterExpression.ExprOpr.newBuilder()
                                    .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE)
                                    .build());
                }
            }
        }
        return exprBuilder.build();
    }

    private boolean needBrace(SqlOperator operator, RexNode operand) {
        return operand instanceof RexCall
                && ((RexCall) operand).getOperator().getLeftPrec() <= operator.getLeftPrec();
    }

    @Override
    public OuterExpression.Expression visitInputRef(RexInputRef inputRef) {
        OuterExpression.Expression.Builder builder = OuterExpression.Expression.newBuilder();
        if (inputRef instanceof RexGraphVariable) {
            RexGraphVariable var = (RexGraphVariable) inputRef;
            OuterExpression.Variable.Builder varBuilder = OuterExpression.Variable.newBuilder();
            if (var.getAliasId() != AliasInference.DEFAULT_ID) {
                varBuilder.setTag(Common.NameOrId.newBuilder().setId(var.getAliasId()).build());
            }
            if (var.getProperty() != null) {
                varBuilder.setProperty(Utils.protoProperty(var.getProperty()));
            }
            DataType.IrDataType varDataType = Utils.protoIrDataType(inputRef.getType(), isColumnId);
            varBuilder.setNodeType(varDataType);
            builder.addOperators(
                    OuterExpression.ExprOpr.newBuilder()
                            .setVar(varBuilder.build())
                            .setNodeType(varDataType)
                            .build());
        }
        return builder.build();
    }

    @Override
    public OuterExpression.Expression visitLiteral(RexLiteral literal) {
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setConst(Utils.protoValue(literal))
                                .setNodeType(Utils.protoIrDataType(literal.getType(), isColumnId))
                                .build())
                .build();
    }

    @Override
    public OuterExpression.Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        DataType.IrDataType paramDataType =
                Utils.protoIrDataType(dynamicParam.getType(), isColumnId);
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setParam(
                                        OuterExpression.DynamicParam.newBuilder()
                                                .setName(dynamicParam.getName())
                                                .setIndex(dynamicParam.getIndex())
                                                .setDataType(paramDataType)
                                                .build())
                                .setNodeType(paramDataType)
                                .build())
                .build();
    }

    @Override
    public OuterExpression.Expression visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException(
                "conversion from subQuery to ir core structure is unsupported yet");
    }
}
