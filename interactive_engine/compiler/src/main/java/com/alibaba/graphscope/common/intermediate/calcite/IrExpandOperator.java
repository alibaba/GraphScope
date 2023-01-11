package com.alibaba.graphscope.common.intermediate.calcite;

import com.alibaba.graphscope.common.intermediate.calcite.clause.type.DirectionOpt;

import jline.internal.Nullable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class IrExpandOperator extends SqlBinaryOperator {
    private DirectionOpt directionOpt;

    public IrExpandOperator(
            String name,
            DirectionOpt directionOpt,
            IrOperatorKind kind,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                0,
                false,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker);
    }

    public DirectionOpt getDirectionOpt() {
        return directionOpt;
    }

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {}

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return null;
    }
}
