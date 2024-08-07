/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/sql/type/FamilyOperandTypeChecker.java
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.type;

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Litmus;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Extends {@code FamilyOperandTypeChecker} to validate operand type(s) based on {@code RexNode} instead of {@code SqlNode},
 * The constructor of {@code FamilyOperandTypeChecker} is protected, To inherit from it,
 * we have to make the subclass under the same package {@code org.apache.calcite.sql.type}
 */
public class GraphFamilyOperandTypeChecker extends FamilyOperandTypeChecker {
    protected final List<RelDataTypeFamily> expectedFamilies;

    protected GraphFamilyOperandTypeChecker(
            List<RelDataTypeFamily> typeFamilies, Predicate<Integer> optional) {
        super(ImmutableList.of(), optional);
        this.expectedFamilies = typeFamilies;
    }

    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (expectedFamilies.size() != callBinding.getOperandCount()) {
            Litmus.THROW.fail(
                    "Expect {} operands, but got {} operands, expected operands of types {}",
                    expectedFamilies.size(),
                    callBinding.getOperandCount(),
                    expectedFamilies);
        }
        if (!(callBinding instanceof RexCallBinding)) {
            throw new IllegalArgumentException(
                    "argument \"callBinding\" in subclass="
                            + this.getClass()
                            + " should be of type="
                            + RexCallBinding.class);
        }
        RexCallBinding rexCallBinding = (RexCallBinding) callBinding;
        for (Ord<RexNode> op : Ord.zip(rexCallBinding.getRexOperands())) {
            if (!checkSingleOperandType(callBinding, op.e, op.i, false)) {
                // try to coerce type if it is allowed.
                boolean coerced = false;
                if (callBinding.isTypeCoercionEnabled()) {
                    TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
                    ImmutableList.Builder<RelDataType> builder = ImmutableList.builder();
                    for (int i = 0; i < callBinding.getOperandCount(); i++) {
                        builder.add(callBinding.getOperandType(i));
                    }
                    ImmutableList<RelDataType> dataTypes = builder.build();
                    coerced =
                            typeCoercion.builtinFunctionCoercion(callBinding, dataTypes, families);
                }
                // re-validate the new nodes type.
                for (Ord<RexNode> op1 : Ord.zip(rexCallBinding.getRexOperands())) {
                    if (!checkSingleOperandType(callBinding, op1.e, op1.i, throwOnFailure)) {
                        return false;
                    }
                }
                return coerced;
            }
        }
        return true;
    }

    public boolean checkOperandTypesWithoutTypeCoercion(
            SqlCallBinding callBinding, boolean throwOnFailure) {
        if (expectedFamilies.size() != callBinding.getOperandCount()) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw exception.
            return false;
        }
        if (!(callBinding instanceof RexCallBinding)) {
            throw new IllegalArgumentException(
                    "callBinding in subclass="
                            + this.getClass()
                            + " should be "
                            + RexCallBinding.class);
        }
        RexCallBinding rexCallBinding = (RexCallBinding) callBinding;
        for (Ord<RexNode> op : Ord.zip(rexCallBinding.getRexOperands())) {
            if (!checkSingleOperandType(callBinding, op.e, op.i, throwOnFailure)) {
                return false;
            }
        }
        return true;
    }

    /**
     * we will never implement this function for it depends on {@code SqlNode} to validate the type
     *
     * @param callBinding
     * @param node
     * @param iFormalOperand
     * @param throwOnFailure
     * @return
     */
    @Override
    public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure) {
        throw new UnsupportedOperationException(
                "checkSingleOperandType is unsupported for we will never depend on SqlNode to check"
                        + " type");
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return SqlUtil.getAliasedSignature(op, opName, this.expectedFamilies);
    }

    protected boolean checkSingleOperandType(
            SqlCallBinding callBinding, RexNode node, int iFormalOperand, boolean throwOnFailure) {
        RelDataTypeFamily expectedFamily = expectedFamilies.get(iFormalOperand);
        RelDataType type = node.getType();
        if (expectedFamily instanceof SqlTypeFamily) {
            SqlTypeFamily sqlTypeFamily = (SqlTypeFamily) expectedFamily;
            switch (sqlTypeFamily) {
                case ANY:
                    SqlTypeName typeName = node.getType().getSqlTypeName();
                    if (typeName == SqlTypeName.CURSOR) {
                        // We do not allow CURSOR operands, even for ANY
                        if (throwOnFailure) {
                            throw callBinding.newValidationSignatureError();
                        }
                        return false;
                    }
                    // fall through
                case IGNORE:
                    // no need to check
                    return true;
                default:
                    break;
            }
            if (isNullLiteral(node)) {
                if (callBinding.isTypeCoercionEnabled()) {
                    return true;
                } else if (throwOnFailure) {
                    throw new IllegalArgumentException(
                            "node " + node + " should not be of null value");
                } else {
                    return false;
                }
            }

            SqlTypeName typeName = type.getSqlTypeName();

            // Pass type checking for operators if it's of type 'ANY'.
            if (typeName.getFamily() == SqlTypeFamily.ANY) {
                return true;
            }

            if (!getAllowedTypeNames(callBinding.getTypeFactory(), sqlTypeFamily, iFormalOperand)
                    .contains(typeName)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        } else {
            if (type.getFamily() == SqlTypeFamily.ANY || expectedFamily == SqlTypeFamily.ANY)
                return true;
            if (type.getFamily() != expectedFamily) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }

    protected Collection<SqlTypeName> getAllowedTypeNames(
            RelDataTypeFactory typeFactory, SqlTypeFamily family, int iFormalOperand) {
        return family.getTypeNames();
    }

    private boolean isNullLiteral(RexNode node) {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            if (literal.getTypeName() == SqlTypeName.NULL) {
                assert null == literal.getValue();
                return true;
            } else {
                return false;
            }
        }
        return false;
    }
}
