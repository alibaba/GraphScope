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
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.DataType;
import com.alibaba.graphscope.gaia.proto.OuterExpression;

import org.apache.calcite.rex.*;
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
        OuterExpression.Expression.Builder exprBuilder = OuterExpression.Expression.newBuilder();
        SqlOperator operator = call.getOperator();
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
                && ((RexCall) operand).getOperator().getLeftPrec() < operator.getLeftPrec();
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
}
