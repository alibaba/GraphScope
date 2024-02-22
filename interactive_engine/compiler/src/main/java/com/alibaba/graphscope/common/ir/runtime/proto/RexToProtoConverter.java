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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Sarg;

import java.util.List;

/**
 * convert an expression in calcite to logical expression in ir_core
 */
public class RexToProtoConverter extends RexVisitorImpl<OuterExpression.Expression> {
    private final boolean isColumnId;
    private final RexBuilder rexBuilder;

    public RexToProtoConverter(boolean deep, boolean isColumnId, RexBuilder rexBuilder) {
        super(deep);
        this.isColumnId = isColumnId;
        this.rexBuilder = rexBuilder;
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
        } else if (operator.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR) {
            return visitMapValueConstructor(call);
        } else if (operator.getKind() == SqlKind.EXTRACT) {
            return visitExtract(call);
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

    private OuterExpression.Expression visitMapValueConstructor(RexCall call) {
        OuterExpression.VariableKeyValues.Builder varMapBuilder =
                OuterExpression.VariableKeyValues.newBuilder();
        List<RexNode> operands = call.getOperands();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            RexNode key = operands.get(i);
            RexNode value = operands.get(i + 1);
            Preconditions.checkArgument(
                    key instanceof RexLiteral,
                    "key type of 'MAP_VALUE_CONSTRUCTOR' should be 'literal', but is "
                            + key.getClass());
            Preconditions.checkArgument(
                    value instanceof RexGraphVariable,
                    "value type of 'MAP_VALUE_CONSTRUCTOR' should be 'variable', but is "
                            + value.getClass());
            varMapBuilder.addKeyVals(
                    OuterExpression.VariableKeyValue.newBuilder()
                            .setKey(key.accept(this).getOperators(0).getConst())
                            .setValue(value.accept(this).getOperators(0).getVar())
                            .build());
        }
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setMap(varMapBuilder)
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)))
                .build();
    }

    private OuterExpression.Expression visitExtract(RexCall call) {
        List<RexNode> operands = call.getOperands();
        Preconditions.checkArgument(
                operands.size() == 2 && operands.get(0) instanceof RexLiteral,
                "'EXTRACT' operator has invalid operands " + operands);
        OuterExpression.Expression.Builder builder = OuterExpression.Expression.newBuilder();
        builder.addOperators(
                OuterExpression.ExprOpr.newBuilder()
                        .setExtract(
                                OuterExpression.Extract.newBuilder()
                                        .setInterval(
                                                Utils.protoInterval((RexLiteral) operands.get(0))))
                        .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)));
        SqlOperator operator = call.getOperator();
        RexNode operand = operands.get(1);
        boolean needBrace = needBrace(operator, operand);
        if (needBrace) {
            builder.addOperators(
                    OuterExpression.ExprOpr.newBuilder()
                            .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE));
        }
        builder.addAllOperators(operand.accept(this).getOperatorsList());
        if (needBrace) {
            builder.addOperators(
                    OuterExpression.ExprOpr.newBuilder()
                            .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE));
        }
        return builder.build();
    }

    private OuterExpression.Expression visitUnaryOperator(RexCall call) {
        SqlOperator operator = call.getOperator();
        RexNode operand = call.getOperands().get(0);
        switch (operator.getKind()) {
                // convert IS_NOT_NULL to NOT(IS_NULL(XX))
            case IS_NOT_NULL:
                return OuterExpression.Expression.newBuilder()
                        .addOperators(
                                Utils.protoOperator(GraphStdOperatorTable.NOT).toBuilder()
                                        .setNodeType(
                                                Utils.protoIrDataType(call.getType(), isColumnId)))
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE))
                        .addAllOperators(
                                visitUnaryOperator(
                                                GraphStdOperatorTable.IS_NULL,
                                                operand,
                                                call.getType())
                                        .getOperatorsList())
                        .addOperators(
                                OuterExpression.ExprOpr.newBuilder()
                                        .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE))
                        .build();
            case IS_NULL:
            case NOT:
            default:
                return visitUnaryOperator(operator, operand, call.getType());
        }
    }

    private OuterExpression.Expression visitUnaryOperator(
            SqlOperator operator, RexNode operand, RelDataType dataType) {
        OuterExpression.Expression.Builder builder = OuterExpression.Expression.newBuilder();
        builder.addOperators(
                Utils.protoOperator(operator).toBuilder()
                        .setNodeType(Utils.protoIrDataType(dataType, isColumnId)));
        boolean needBrace = needBrace(operator, operand);
        if (needBrace) {
            builder.addOperators(
                    OuterExpression.ExprOpr.newBuilder()
                            .setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE));
        }
        builder.addAllOperators(operand.accept(this).getOperatorsList());
        if (needBrace) {
            builder.addOperators(
                    OuterExpression.ExprOpr.newBuilder()
                            .setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE));
        }
        return builder.build();
    }

    private OuterExpression.Expression visitBinaryOperator(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.SEARCH) {
            // ir core cannot support continuous ranges in a search operator, here expand it to
            // compositions of 'and' or 'or',
            // i.e. a.age SEARCH [[1, 10]] -> a.age >= 1 and a.age <= 10
            RexNode left = call.getOperands().get(0);
            RexNode right = call.getOperands().get(1);
            RexLiteral literal = null;
            if (left instanceof RexLiteral) {
                literal = (RexLiteral) left;
            } else if (right instanceof RexLiteral) {
                literal = (RexLiteral) right;
            }
            if (literal != null && literal.getValue() instanceof Sarg) {
                Sarg sarg = (Sarg) literal.getValue();
                // search continuous ranges
                if (!sarg.isPoints()) {
                    call = (RexCall) RexUtil.expandSearch(this.rexBuilder, null, call);
                }
            }
        }
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
