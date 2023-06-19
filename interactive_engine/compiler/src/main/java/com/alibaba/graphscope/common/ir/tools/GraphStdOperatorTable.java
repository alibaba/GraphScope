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

import org.apache.calcite.sql.*;
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
}
