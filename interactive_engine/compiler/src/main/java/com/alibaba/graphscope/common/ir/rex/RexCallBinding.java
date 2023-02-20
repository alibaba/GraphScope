/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/rex/RexCallBinding.java
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rex;

import static com.alibaba.graphscope.common.ir.util.Static.RESOURCE;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends {@link org.apache.calcite.sql.SqlCallBinding} by referring to an underlying collection of {@link RexNode} operands
 */
public class RexCallBinding extends AbstractCallBinding {
    private List<RexNode> rexOperands;
    private List<RelCollation> inputCollations;

    public RexCallBinding(
            RelDataTypeFactory typeFactory,
            SqlOperator operator,
            List<RexNode> operands,
            List<RelCollation> inputCollations) {
        super(typeFactory, operator);
        this.rexOperands = operands;
        this.inputCollations = inputCollations;
    }

    // implement SqlOperatorBinding
    @Override
    public @Nullable String getStringLiteralOperand(int ordinal) {
        return RexLiteral.stringValue(rexOperands.get(ordinal));
    }

    @Override
    public int getIntLiteralOperand(int ordinal) {
        return RexLiteral.intValue(rexOperands.get(ordinal));
    }

    @Override
    public <T> @Nullable T getOperandLiteralValue(int ordinal, Class<T> clazz) {
        final RexNode node = rexOperands.get(ordinal);
        if (node instanceof RexLiteral) {
            return ((RexLiteral) node).getValueAs(clazz);
        }
        return clazz.cast(RexLiteral.value(node));
    }

    @Override
    public boolean isOperandNull(int ordinal, boolean allowCast) {
        return RexUtil.isNullLiteral(rexOperands.get(ordinal), allowCast);
    }

    @Override
    public boolean isOperandLiteral(int ordinal, boolean allowCast) {
        return RexUtil.isLiteral(rexOperands.get(ordinal), allowCast);
    }

    @Override
    public int getOperandCount() {
        return rexOperands.size();
    }

    @Override
    public RelDataType getOperandType(int ordinal) {
        return rexOperands.get(ordinal).getType();
    }

    @Override
    public SqlMonotonicity getOperandMonotonicity(int ordinal) {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }

    // override SqlCallBinding
    @Override
    public CalciteException newValidationSignatureError() {
        return RESOURCE.canNotApplyOpToOperands(
                        getOperator().getName(),
                        getCallSignature(),
                        getOperator().getAllowedSignatures())
                .ex();
    }

    @Override
    public CalciteException newValidationError(Resources.ExInst<SqlValidatorException> ex) {
        Throwable t = ex.ex();
        return new CalciteException(t.getMessage(), t);
    }

    private String getCallSignature() {
        List<String> signatureList = new ArrayList<>();
        for (RexNode operand : getRexOperands()) {
            RelDataType argType = operand.getType();
            if (null == argType) {
                continue;
            }
            signatureList.add(argType.toString());
        }
        return SqlUtil.getOperatorSignature(getOperator(), signatureList);
    }

    public List<RexNode> getRexOperands() {
        return rexOperands;
    }
}
