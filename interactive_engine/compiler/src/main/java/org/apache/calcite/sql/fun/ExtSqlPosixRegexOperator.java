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

package org.apache.calcite.sql.fun;

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

/**
 * The operator is used for regex match for string values, i.e a.name like '%marko' in a sql expression.
 * The original implementation will check operand types by {@link org.apache.calcite.sql.SqlCall}, which is a structure in sql parser phase.
 * Here we override the interface to check types by {@link org.apache.calcite.rex.RexCall} which represents an algebra relation.
 */
public class ExtSqlPosixRegexOperator extends SqlPosixRegexOperator {
    public ExtSqlPosixRegexOperator(
            String name, SqlKind kind, boolean caseSensitive, boolean negated) {
        super(name, kind, caseSensitive, negated);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        int operandCount = callBinding.getOperandCount();
        if (operandCount != 2) {
            throw new AssertionError(
                    "Unexpected number of args to " + callBinding.getCall() + ": " + operandCount);
        } else {
            RelDataType op1Type = callBinding.getOperandType(0);
            RelDataType op2Type = callBinding.getOperandType(1);
            if (!SqlTypeUtil.isComparable(op1Type, op2Type)) {
                throw new AssertionError(
                        "Incompatible first two operand types " + op1Type + " and " + op2Type);
            } else {
                if (!SqlTypeUtil.isCharTypeComparable(callBinding.collectOperandTypes())) {
                    if (throwOnFailure) {
                        String msg =
                                String.join(
                                        ", ",
                                        Util.transform(
                                                ((RexCallBinding) callBinding).getRexOperands(),
                                                String::valueOf));
                        throw callBinding.newError(Static.RESOURCE.operandNotComparable(msg));
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            }
        }
    }
}
