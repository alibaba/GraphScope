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

package com.alibaba.graphscope.common.ir.rex.operator;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.List;

public class CaseOperator extends SqlOperator {

    public CaseOperator(SqlOperandTypeInference operandTypeInference) {
        super("CASE", SqlKind.CASE, MDX_PRECEDENCE, true, null, operandTypeInference, null);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        Preconditions.checkArgument(callBinding instanceof RexCallBinding);
        boolean foundNotNull = false;
        int operandCount = callBinding.getOperandCount();
        for (int i = 0; i < operandCount - 1; ++i) {
            RelDataType type = callBinding.getOperandType(i);
            if ((i & 1) == 0) { // when expression, should be boolean
                if (!SqlTypeUtil.inBooleanFamily(type)) {
                    if (throwOnFailure) {
                        throw new IllegalArgumentException(
                                "Expected a boolean type at operand idx = " + i);
                    }
                    return false;
                }
            } else { // then expression
                if (!callBinding.isOperandNull(i, false)) {
                    foundNotNull = true;
                }
            }
        }

        if (operandCount > 2 && !callBinding.isOperandNull(operandCount - 1, false)) {
            foundNotNull = true;
        }

        if (!foundNotNull) {
            // according to the sql standard we cannot have all of the THEN
            // statements and the ELSE returning null
            if (throwOnFailure && !callBinding.isTypeCoercionEnabled()) {
                throw callBinding.newValidationError(RESOURCE.mustNotNullInElse());
            }
            return false;
        }
        return true;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return inferTypeFromOperands(opBinding);
    }

    private static RelDataType inferTypeFromOperands(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final List<RelDataType> argTypes = opBinding.collectOperandTypes();
        assert (argTypes.size() % 2) == 1 : "odd number of arguments expected: " + argTypes.size();
        assert argTypes.size() > 1
                : "CASE must have more than 1 argument. Given " + argTypes.size() + ", " + argTypes;
        List<RelDataType> thenTypes = new ArrayList<>();
        for (int j = 1; j < (argTypes.size() - 1); j += 2) {
            RelDataType argType = argTypes.get(j);
            if (argType != null && argType.getSqlTypeName() != SqlTypeName.NULL) {
                thenTypes.add(argType);
            }
        }
        RelDataType lastType = Iterables.getLast(argTypes);
        if (lastType != null && lastType.getSqlTypeName() != SqlTypeName.NULL) {
            thenTypes.add(lastType);
        }
        return requireNonNull(
                typeFactory.leastRestrictive(thenTypes),
                () -> "Can't find leastRestrictive type for " + thenTypes);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }
}
