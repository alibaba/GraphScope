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

import com.alibaba.graphscope.common.ir.meta.function.GraphFunctions;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.DataType;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

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
        } else if (operator.getKind() == SqlKind.OTHER
                && operator.getName().equals("PATH_CONCAT")) {
            return visitPathConcat(call);
        } else if (operator.getKind() == SqlKind.OTHER
                && operator.getName().equals("PATH_FUNCTION")) {
            return visitPathFunction(call);
        } else if (operator.getKind() == SqlKind.EXTRACT) {
            return visitExtract(call);
        } else if (operator.getKind() == SqlKind.OTHER
                && operator.getName().equals("DATETIME_MINUS")) {
            return visitDateMinus(call);
        } else if (operator.getKind() == SqlKind.OTHER
                && operator.getName().startsWith(GraphFunctions.FUNCTION_PREFIX)) {
            return visitUserDefinedFunction(call);
        } else if (call.getOperands().size() == 1) {
            return visitUnaryOperator(call);
        } else {
            return visitBinaryOperator(call);
        }
    }

    private OuterExpression.Expression visitUserDefinedFunction(RexCall call) {
        List<RexNode> operands = call.getOperands();
        List<OuterExpression.Expression> parameters =
                operands.stream().map(operand -> operand.accept(this)).collect(Collectors.toList());
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setUdfFunc(
                                        OuterExpression.UserDefinedFunction.newBuilder()
                                                .setName(call.op.getName())
                                                .addAllParameters(parameters)
                                                .build())
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)))
                .build();
    }

    private OuterExpression.Expression visitPathFunction(RexCall call) {
        List<RexNode> operands = call.getOperands();
        RexGraphVariable variable = (RexGraphVariable) operands.get(0);
        OuterExpression.PathFunction.Builder builder = OuterExpression.PathFunction.newBuilder();
        if (variable.getAliasId() != AliasInference.DEFAULT_ID) {
            builder.setTag(Common.NameOrId.newBuilder().setId(variable.getAliasId()).build());
        }
        GraphOpt.PathExpandFunction funcOpt =
                ((RexLiteral) operands.get(1)).getValueAs(GraphOpt.PathExpandFunction.class);
        builder.setOpt(OuterExpression.PathFunction.FuncOpt.valueOf(funcOpt.name()));
        setPathFunctionProjection(builder, operands.get(2));
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setPathFunc(builder)
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)))
                .build();
    }

    private void setPathFunctionProjection(
            OuterExpression.PathFunction.Builder builder, RexNode projection) {
        switch (projection.getKind()) {
            case MAP_VALUE_CONSTRUCTOR:
                List<RexNode> operands = ((RexCall) projection).getOperands();
                OuterExpression.PathFunction.PathElementKeyValues.Builder keyValuesBuilder =
                        OuterExpression.PathFunction.PathElementKeyValues.newBuilder();
                for (int i = 0; i < operands.size(); i += 2) {
                    keyValuesBuilder.addKeyVals(
                            OuterExpression.PathFunction.PathElementKeyValues.PathElementKeyValue
                                    .newBuilder()
                                    .setKey(operands.get(i).accept(this).getOperators(0).getConst())
                                    .setVal(visitPathFunctionProperty(operands.get(i + 1))));
                }
                builder.setMap(keyValuesBuilder);
                break;
            case ARRAY_VALUE_CONSTRUCTOR:
                OuterExpression.PathFunction.PathElementKeys.Builder valuesBuilder =
                        OuterExpression.PathFunction.PathElementKeys.newBuilder();
                ((RexCall) projection)
                        .getOperands()
                        .forEach(
                                operand ->
                                        valuesBuilder.addKeys(visitPathFunctionProperty(operand)));
                builder.setVars(valuesBuilder);
                break;
            default:
                builder.setProperty(visitPathFunctionProperty(projection));
        }
    }

    private OuterExpression.Property visitPathFunctionProperty(RexNode variable) {
        Preconditions.checkArgument(
                variable instanceof RexGraphVariable
                        && ((RexGraphVariable) variable).getProperty() != null,
                "cannot get path function property from variable [" + variable + "]");
        return Utils.protoProperty(((RexGraphVariable) variable).getProperty());
    }

    private OuterExpression.Expression visitPathConcat(RexCall call) {
        List<RexNode> operands = call.getOperands();
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setPathConcat(
                                        OuterExpression.PathConcat.newBuilder()
                                                .setLeft(convertPathInfo(operands))
                                                .setRight(
                                                        convertPathInfo(
                                                                operands.subList(
                                                                        2, operands.size())))))
                .build();
    }

    private OuterExpression.PathConcat.ConcatPathInfo convertPathInfo(List<RexNode> operands) {
        Preconditions.checkArgument(
                operands.size() >= 2
                        && operands.get(0) instanceof RexGraphVariable
                        && operands.get(1) instanceof RexLiteral);
        OuterExpression.Variable variable = operands.get(0).accept(this).getOperators(0).getVar();
        GraphOpt.GetV direction = ((RexLiteral) operands.get(1)).getValueAs(GraphOpt.GetV.class);
        return OuterExpression.PathConcat.ConcatPathInfo.newBuilder()
                .setPathTag(variable)
                .setEndpoint(convertPathDirection(direction))
                .build();
    }

    private OuterExpression.PathConcat.Endpoint convertPathDirection(GraphOpt.GetV direction) {
        switch (direction) {
            case START:
                return OuterExpression.PathConcat.Endpoint.START;
            case END:
                return OuterExpression.PathConcat.Endpoint.END;
            default:
                throw new IllegalArgumentException(
                        "invalid path concat direction ["
                                + direction.name()
                                + "], cannot convert to any physical expression");
        }
    }

    private OuterExpression.Expression visitDateMinus(RexCall call) {
        OuterExpression.Expression.Builder exprBuilder = OuterExpression.Expression.newBuilder();
        RexLiteral interval = (RexLiteral) call.getOperands().get(2);
        SqlOperator operator = call.getOperator();
        for (int i = 0; i < 2; ++i) {
            RexNode operand = call.getOperands().get(i);
            if (i != 0) {
                exprBuilder.addOperators(
                        OuterExpression.ExprOpr.newBuilder()
                                .setDateTimeMinus(
                                        OuterExpression.DateTimeMinus.newBuilder()
                                                .setInterval(Utils.protoInterval(interval)))
                                .setNodeType(Utils.protoIrDataType(call.getType(), isColumnId)));
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
        return exprBuilder.build();
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
            OuterExpression.ExprOpr valueOpr = value.accept(this).getOperators(0);
            OuterExpression.VariableKeyValue.Builder keyValueBuilder =
                    OuterExpression.VariableKeyValue.newBuilder()
                            .setKey(key.accept(this).getOperators(0).getConst());
            switch (valueOpr.getItemCase()) {
                case VAR:
                    keyValueBuilder.setVal(valueOpr.getVar());
                    break;
                case MAP:
                    keyValueBuilder.setNested(valueOpr.getMap());
                    break;
                case PATH_FUNC:
                    keyValueBuilder.setPathFunc(valueOpr.getPathFunc());
                    break;
                default:
                    throw new IllegalArgumentException(
                            "can not convert value ["
                                    + value
                                    + "] of 'MAP_VALUE_CONSTRUCTOR' to physical plan");
            }
            varMapBuilder.addKeyVals(keyValueBuilder);
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
                // if the operand of MINUS_PREFIX is a literal, we can convert it to a negative
                // value
            case MINUS_PREFIX:
                if (operand.getKind() == SqlKind.LITERAL) {
                    RexLiteral literal = (RexLiteral) operand;
                    switch (literal.getType().getSqlTypeName()) {
                        case INTEGER:
                            BigInteger negative =
                                    BigInteger.valueOf(literal.getValueAs(Number.class).intValue())
                                            .negate();
                            if (negative.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0
                                    && negative.compareTo(BigInteger.valueOf(Integer.MAX_VALUE))
                                            <= 0) {
                                return OuterExpression.Expression.newBuilder()
                                        .addOperators(
                                                OuterExpression.ExprOpr.newBuilder()
                                                        .setConst(
                                                                Common.Value.newBuilder()
                                                                        .setI32(
                                                                                negative
                                                                                        .intValue()))
                                                        .setNodeType(
                                                                Utils.protoIrDataType(
                                                                        call.getType(),
                                                                        isColumnId)))
                                        .build();
                            }
                        case BIGINT:
                            BigInteger negative2 =
                                    BigInteger.valueOf(literal.getValueAs(Number.class).longValue())
                                            .negate();
                            if (negative2.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0
                                    && negative2.compareTo(BigInteger.valueOf(Long.MAX_VALUE))
                                            <= 0) {
                                return OuterExpression.Expression.newBuilder()
                                        .addOperators(
                                                OuterExpression.ExprOpr.newBuilder()
                                                        .setConst(
                                                                Common.Value.newBuilder()
                                                                        .setI64(
                                                                                negative2
                                                                                        .longValue()))
                                                        .setNodeType(
                                                                Utils.protoIrDataType(
                                                                        rexBuilder
                                                                                .getTypeFactory()
                                                                                .createSqlType(
                                                                                        SqlTypeName
                                                                                                .BIGINT),
                                                                        isColumnId)))
                                        .build();
                            } else {
                                throw new IllegalArgumentException(
                                        "negation of value ["
                                                + negative2
                                                + "] is out of range of BIGINT");
                            }
                        case FLOAT:
                        case DOUBLE:
                            BigDecimal negative3 =
                                    BigDecimal.valueOf(
                                                    literal.getValueAs(Number.class).doubleValue())
                                            .negate();
                            return OuterExpression.Expression.newBuilder()
                                    .addOperators(
                                            OuterExpression.ExprOpr.newBuilder()
                                                    .setConst(
                                                            Common.Value.newBuilder()
                                                                    .setF64(
                                                                            negative3
                                                                                    .doubleValue()))
                                                    .setNodeType(
                                                            Utils.protoIrDataType(
                                                                    rexBuilder
                                                                            .getTypeFactory()
                                                                            .createSqlType(
                                                                                    SqlTypeName
                                                                                            .DOUBLE),
                                                                    isColumnId)))
                                    .build();
                    }
                }
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
        switch (operator.getKind()) {
            case NOT:
                // the operator precedence of `not` in sql is low, which is the same as `and` or
                // `or`.
                // However, the `not` operator is implemented by `!` in c++ or in rust, which
                // precedence is high.
                // To solve the inconsistency, we always add brace for operand of `not` operator.
                return true;
            default:
                return operand instanceof RexCall
                        && ((RexCall) operand).getOperator().getLeftPrec()
                                <= operator.getLeftPrec();
        }
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
        OuterExpression.ExprOpr.Builder oprBuilder = OuterExpression.ExprOpr.newBuilder();
        if (literal.getType() instanceof IntervalSqlType) {
            // convert to time interval
            oprBuilder.setTimeInterval(
                    OuterExpression.TimeInterval.newBuilder()
                            .setInterval(Utils.protoInterval(literal))
                            .setConst(
                                    Common.Value.newBuilder()
                                            .setI64(literal.getValueAs(Number.class).longValue())));
        } else {
            oprBuilder.setConst(Utils.protoValue(literal));
        }
        return OuterExpression.Expression.newBuilder()
                .addOperators(
                        oprBuilder
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
        throw new IllegalArgumentException(
                "cannot convert sub query ["
                        + subQuery
                        + "] to physical expression, please use 'apply' instead");
    }
}
