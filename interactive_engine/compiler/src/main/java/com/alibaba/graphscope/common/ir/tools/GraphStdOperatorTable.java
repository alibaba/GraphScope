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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.function.FunctionMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.rex.operator.CaseOperator;
import com.alibaba.graphscope.common.ir.rex.operator.SqlArrayValueConstructor;
import com.alibaba.graphscope.common.ir.rex.operator.SqlMapValueConstructor;
import com.alibaba.graphscope.common.ir.type.GraphTypeFamily;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.ExtSqlPosixRegexOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;

/**
 * Extends {@link org.apache.calcite.sql.fun.SqlStdOperatorTable} to re-implement type checker/inference in some operators
 */
public class GraphStdOperatorTable extends SqlStdOperatorTable {
    public static final SqlBinaryOperator PLUS =
            new SqlMonotonicBinaryOperator(
                    "+",
                    SqlKind.PLUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.PLUS_OPERATOR);

    public static final SqlBinaryOperator MINUS =
            new SqlMonotonicBinaryOperator(
                    "-",
                    SqlKind.MINUS,
                    40,
                    true,

                    // Same type inference strategy as sum
                    ReturnTypes.NULLABLE_SUM,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.MINUS_OPERATOR);

    public static final SqlOperator DATETIME_MINUS =
            new SqlSpecialOperator(
                    "DATETIME_MINUS",
                    SqlKind.OTHER,
                    40,
                    true,
                    ReturnTypes.ARG2_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    GraphOperandTypes.DATETIME_DATETIME_INTERVAL);

    public static final SqlBinaryOperator BIT_AND =
            new SqlMonotonicBinaryOperator(
                    "&",
                    SqlKind.BIT_AND,
                    40,
                    true,
                    ReturnTypes.ARG0,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.NUMERIC_NUMERIC);

    public static final SqlBinaryOperator BIT_OR =
            new SqlMonotonicBinaryOperator(
                    "|",
                    SqlKind.BIT_OR,
                    40,
                    true,
                    ReturnTypes.ARG0,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.NUMERIC_NUMERIC);

    public static final SqlBinaryOperator BIT_XOR =
            new SqlMonotonicBinaryOperator(
                    "^",
                    SqlKind.BIT_XOR,
                    40,
                    true,
                    ReturnTypes.ARG0,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.NUMERIC_NUMERIC);

    public static final SqlBinaryOperator MULTIPLY =
            new SqlMonotonicBinaryOperator(
                    "*",
                    SqlKind.TIMES,
                    60,
                    true,
                    ReturnTypes.PRODUCT_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.MULTIPLY_OPERATOR);

    public static final SqlBinaryOperator DIVIDE =
            new SqlBinaryOperator(
                    "/",
                    SqlKind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.QUOTIENT_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.DIVISION_OPERATOR);

    public static final SqlFunction MOD =
            // Return type is same as divisor (2nd operand)
            // SQL2003 Part2 Section 6.27, Syntax Rules 9
            new SqlFunction(
                    "MOD",
                    SqlKind.MOD,
                    ReturnTypes.NULLABLE_MOD,
                    null,
                    GraphOperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlBinaryOperator AND =
            new SqlBinaryOperator(
                    "AND",
                    SqlKind.AND,
                    24,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    GraphOperandTypes.BOOLEAN_BOOLEAN);

    public static final SqlBinaryOperator OR =
            new SqlBinaryOperator(
                    "OR",
                    SqlKind.OR,
                    22,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    GraphOperandTypes.BOOLEAN_BOOLEAN);

    public static final SqlFunction POWER =
            new SqlFunction(
                    "POWER",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    GraphOperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction BIT_LEFT_SHIFT =
            new SqlFunction(
                    "<<",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlFunction BIT_RIGHT_SHIFT =
            new SqlFunction(
                    ">>",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    GraphInferTypes.FIRST_KNOWN,
                    GraphOperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);

    public static final SqlPrefixOperator UNARY_MINUS =
            new SqlPrefixOperator(
                    "-",
                    SqlKind.MINUS_PREFIX,
                    80,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    GraphOperandTypes.NUMERIC_OR_INTERVAL);

    public static final SqlBinaryOperator EQUALS =
            new SqlBinaryOperator(
                    "=",
                    SqlKind.EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    public static final SqlBinaryOperator NOT_EQUALS =
            new SqlBinaryOperator(
                    "<>",
                    SqlKind.NOT_EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);

    public static final SqlBinaryOperator GREATER_THAN =
            new SqlBinaryOperator(
                    ">",
                    SqlKind.GREATER_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    ">=",
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    public static final SqlBinaryOperator LESS_THAN =
            new SqlBinaryOperator(
                    "<",
                    SqlKind.LESS_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    "<=",
                    SqlKind.LESS_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

    public static final SqlOperator CASE = new CaseOperator(GraphInferTypes.RETURN_TYPE);

    public static final SqlFunction USER_DEFINED_PROCEDURE(StoredProcedureMeta meta) {
        SqlReturnTypeInference returnTypeInference = ReturnTypes.explicit(meta.getReturnType());
        SqlOperandTypeChecker operandTypeChecker = GraphOperandTypes.metaTypeChecker(meta);
        return new SqlFunction(
                meta.getName(),
                SqlKind.PROCEDURE_CALL,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_PROCEDURE);
    }

    public static final SqlFunction USER_DEFINED_FUNCTION(FunctionMeta meta) {
        return new SqlFunction(
                meta.getSignature(),
                SqlKind.OTHER,
                meta.getReturnTypeInference(),
                meta.getOperandTypeInference(),
                meta.getOperandTypeChecker(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    // combine multiple expressions into a list
    public static final SqlOperator ARRAY_VALUE_CONSTRUCTOR = new SqlArrayValueConstructor();

    // combine multiple expressions into a map
    public static final SqlOperator MAP_VALUE_CONSTRUCTOR = new SqlMapValueConstructor();

    public static final SqlFunction EXTRACT =
            new SqlFunction(
                    "EXTRACT",
                    SqlKind.EXTRACT,
                    ReturnTypes.BIGINT_NULLABLE,
                    null,
                    GraphOperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
                    SqlFunctionCategory.SYSTEM);

    public static final SqlOperator POSIX_REGEX_CASE_SENSITIVE =
            new ExtSqlPosixRegexOperator(
                    "POSIX REGEX CASE SENSITIVE", SqlKind.POSIX_REGEX_CASE_SENSITIVE, true, false);

    public static final SqlOperator IN =
            new SqlBinaryOperator(
                    "IN",
                    SqlKind.OTHER,
                    32,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    GraphInferTypes.IN_OPERANDS_TYPE,
                    GraphOperandTypes.ANY_ANY);

    public static final SqlOperator PATH_CONCAT =
            new SqlFunction(
                    "PATH_CONCAT",
                    SqlKind.OTHER,
                    ReturnTypes.ARG0,
                    null,
                    GraphOperandTypes.operandMetadata(
                            ImmutableList.of(
                                    GraphTypeFamily.PATH,
                                    SqlTypeFamily.IGNORE,
                                    GraphTypeFamily.PATH,
                                    SqlTypeFamily.IGNORE),
                            typeFactory -> ImmutableList.of(),
                            i ->
                                    ImmutableList.of(
                                                    "LeftPath",
                                                    "LeftDirection",
                                                    "RightPath",
                                                    "RightDirection")
                                            .get(i),
                            i -> false),
                    SqlFunctionCategory.SYSTEM);

    public static final SqlOperator PATH_FUNCTION =
            new SqlFunction(
                    "PATH_FUNCTION",
                    SqlKind.OTHER,
                    ReturnTypes.ARG2.andThen(SqlTypeTransforms.TO_ARRAY),
                    null,
                    GraphOperandTypes.operandMetadata(
                            ImmutableList.of(
                                    GraphTypeFamily.PATH, SqlTypeFamily.IGNORE, SqlTypeFamily.ANY),
                            typeFactory -> ImmutableList.of(),
                            i -> ImmutableList.of("Path", "FuncOpt", "PropertyProjection").get(i),
                            i -> false),
                    SqlFunctionCategory.SYSTEM);
}
