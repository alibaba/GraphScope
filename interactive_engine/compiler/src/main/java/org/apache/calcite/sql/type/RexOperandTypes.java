/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/sql/type/OperandTypes.java
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.type;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

/**
 * Similar to {@link OperandTypes}, but we rewrite some {@link SqlOperandTypeChecker}
 * to create validation functions based on {@link org.apache.calcite.rex.RexNode},
 * Meanwhile, we re-use functions from {@link OperandTypes} as much as possible
 */
public abstract class RexOperandTypes {
    public static final SqlSingleOperandTypeChecker NUMERIC_NUMERIC =
            family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);

    public static final SqlSingleOperandTypeChecker EXACT_NUMERIC_EXACT_NUMERIC =
            family(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.EXACT_NUMERIC);

    public static final SqlSingleOperandTypeChecker NUMERIC = family(SqlTypeFamily.NUMERIC);

    public static final SqlSingleOperandTypeChecker INTERVAL =
            family(SqlTypeFamily.DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker NUMERIC_OR_INTERVAL =
            OperandTypes.or(NUMERIC, INTERVAL);

    public static final FamilyOperandTypeChecker INTERVAL_INTERVAL =
            family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker INTERVAL_SAME_SAME =
            OperandTypes.and(INTERVAL_INTERVAL, OperandTypes.SAME_SAME);

    public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL =
            family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker INTERVAL_DATETIME =
            family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME);

    public static final SqlSingleOperandTypeChecker INTERVAL_NUMERIC =
            family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.NUMERIC);

    public static final SqlSingleOperandTypeChecker NUMERIC_INTERVAL =
            family(SqlTypeFamily.NUMERIC, SqlTypeFamily.DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker BOOLEAN_BOOLEAN =
            family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN);

    /**
     * create {@code RexFamilyOperandTypeChecker} to validate type based on {@code RexNode}
     * @param families
     * @return
     */
    public static FamilyOperandTypeChecker family(SqlTypeFamily... families) {
        return new RexFamilyOperandTypeChecker(ImmutableList.copyOf(families), i -> false);
    }

    public static final SqlSingleOperandTypeChecker PLUS_OPERATOR =
            OperandTypes.or(
                    NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL, INTERVAL_DATETIME);

    public static final SqlSingleOperandTypeChecker MINUS_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker MULTIPLY_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_NUMERIC, NUMERIC_INTERVAL);

    public static final SqlSingleOperandTypeChecker DIVISION_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_NUMERIC);

    public static SqlOperandMetadata operandMetadata(List<SqlTypeFamily> families, Function<RelDataTypeFactory, List<RelDataType>> typesFactory, IntFunction<String> operandName, Predicate<Integer> optional) {
        return new RexOperandMetaDataImpl(families, typesFactory, operandName, optional);
    }
}
