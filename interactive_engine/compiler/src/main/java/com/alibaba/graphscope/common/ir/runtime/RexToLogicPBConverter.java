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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.type.*;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.DataType;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;

/**
 * convert expression in calcite to logical pb in ir_core
 */
public class RexToLogicPBConverter extends RexVisitorImpl<Boolean> {
    private final OuterExpression.Expression.Builder exprBuilder;

    public RexToLogicPBConverter(boolean deep) {
        super(deep);
        this.exprBuilder = OuterExpression.Expression.newBuilder();
    }

    @Override
    public Boolean visitCall(RexCall call) {
        if (!this.deep) {
            return null;
        }
        SqlOperator operator = call.getOperator();
        // left-associative
        if (operator.getLeftPrec() <= operator.getRightPrec()) {
            for (int i = 0; i < call.getOperands().size(); ++i) {
                RexNode operand = call.getOperands().get(i);
                if (i != 0) {
                    exprBuilder.addOperators(protoOperator(operator));
                }
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder().setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE).build());
                }
                operand.accept(this);
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder().setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE).build());
                }
            }
        } else { // right-associative
            for (int i = call.getOperands().size() - 1; i >= 0; --i) {
                RexNode operand = call.getOperands().get(i);
                if (i != 0) {
                    exprBuilder.addOperators(protoOperator(operator));
                }
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder().setBrace(OuterExpression.ExprOpr.Brace.LEFT_BRACE).build());
                }
                operand.accept(this);
                if (needBrace(operator, operand)) {
                    exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder().setBrace(OuterExpression.ExprOpr.Brace.RIGHT_BRACE).build());
                }
            }
        }
        return true;
    }

    private boolean needBrace(SqlOperator operator, RexNode operand) {
        return operand instanceof RexCall
                && ((RexCall) operand).getOperator().getLeftPrec() < operator.getLeftPrec();
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        if (inputRef instanceof RexGraphVariable) {
            RexGraphVariable var = (RexGraphVariable) inputRef;
            exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder()
                    .setVar(OuterExpression.Variable.newBuilder()
                                    .setTag(Common.NameOrId.newBuilder().setId(var.getAliasId()).build())
                                    .setProperty(protoProperty(var.getProperty()))
                                    .build()).build());
        }
        return true;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
        exprBuilder.addOperators(OuterExpression.ExprOpr.newBuilder().setConst(protoValue(literal)).setNodeType(
                DataType.IrDataType.newBuilder().build()
        ).build());
        return true;
    }

    public OuterExpression.Expression.Builder getExprBuilder() {
        return exprBuilder;
    }

    private Common.Value protoValue(RexLiteral literal) {
        switch (literal.getTypeName()) {
            case BOOLEAN:
                return Common.Value.newBuilder().setBoolean((Boolean) literal.getValue()).build();
            case INTEGER:
                return Common.Value.newBuilder().setI32((Integer) literal.getValue()).build();
            case BIGINT:
                return Common.Value.newBuilder().setI64((Long) literal.getValue()).build();
            case DECIMAL:
                BigDecimal decimal = (BigDecimal) literal.getValue();
                return (literal.getType().getSqlTypeName() == SqlTypeName.INTEGER) ?
                        Common.Value.newBuilder().setI32(decimal.intValue()).build() :
                        Common.Value.newBuilder().setI64(decimal.longValue()).build();
            case CHAR:
                return Common.Value.newBuilder().setStr((String) literal.getValue()).build();
            case FLOAT:
            case DOUBLE:
                return Common.Value.newBuilder().setF64((Double) literal.getValue()).build();
            default:
                // TODO: support int/double/string array
                throw new UnsupportedOperationException("literal type " + literal.getTypeName() + " is unsupported yet");
        }
    }

    private OuterExpression.Property protoProperty(GraphProperty property) {
        switch (property.getOpt()) {
            case ID:
                return OuterExpression.Property.newBuilder().setId(OuterExpression.IdKey.newBuilder().build()).build();
            case LABEL:
                return OuterExpression.Property.newBuilder().setLabel(OuterExpression.LabelKey.newBuilder().build()).build();
            case LEN:
                return OuterExpression.Property.newBuilder().setLen(OuterExpression.LengthKey.newBuilder().build()).build();
            case ALL:
                return OuterExpression.Property.newBuilder().setAll(OuterExpression.AllKey.newBuilder().build()).build();
            case KEY:
            default:
                return OuterExpression.Property.newBuilder().setKey(protoNameOrId(property.getKey())).build();
        }
    }

    private Common.NameOrId protoNameOrId(GraphNameOrId nameOrId) {
        switch (nameOrId.getOpt()) {
            case NAME:
                return Common.NameOrId.newBuilder().setName(nameOrId.getName()).build();
            case ID :
            default:
                return Common.NameOrId.newBuilder().setId(nameOrId.getId()).build();
        }
    }

    private OuterExpression.ExprOpr protoOperator(SqlOperator operator) {
        switch (operator.getKind()) {
            case PLUS:
                return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.ADD).build();
            case MINUS:
                return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.SUB).build();
            case TIMES:
                return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.MUL).build();
            case DIVIDE:
                return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.DIV).build();
            case MOD:
                return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.MOD).build();
            case OTHER_FUNCTION:
                if (operator.getName().equals("POWER")) {
                    return OuterExpression.ExprOpr.newBuilder().setArith(OuterExpression.Arithmetic.EXP).build();
                }
            case EQUALS:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.EQ).build();
            case NOT_EQUALS:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.NE).build();
            case GREATER_THAN:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.GT).build();
            case GREATER_THAN_OR_EQUAL:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.GE).build();
            case LESS_THAN:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.LT).build();
            case LESS_THAN_OR_EQUAL:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.LE).build();
            case AND:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.AND).build();
            case OR:
                return OuterExpression.ExprOpr.newBuilder().setLogical(OuterExpression.Logical.OR).build();
            default:
                // TODO: support IN and NOT_IN
                throw new UnsupportedOperationException("operator type=" + operator.getKind() + ", name=" + operator.getName() + " is unsupported yet");
        }
    }

    private DataType.IrDataType protoDataType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                return DataType.IrDataType.newBuilder().setDataType(Common.DataType.BOOLEAN).build();
            case INTEGER:
                return DataType.IrDataType.newBuilder().setDataType(Common.DataType.INT32).build();
            case BIGINT:
            case DECIMAL:
                return DataType.IrDataType.newBuilder().setDataType(Common.DataType.INT64).build();
            case CHAR:
                return DataType.IrDataType.newBuilder().setDataType(Common.DataType.STRING).build();
            case FLOAT:
            case DOUBLE:
                return DataType.IrDataType.newBuilder().setDataType(Common.DataType.DOUBLE).build();
            case ROW:
                if (type instanceof GraphSchemaTypeList) {
                    GraphSchemaTypeList typeList = (GraphSchemaTypeList) type;
                } else if (type instanceof GraphSchemaType) {
                }
            case ARRAY:
                // TODO: support int/double/string/graph-element array
            default:
        }
    }
}
