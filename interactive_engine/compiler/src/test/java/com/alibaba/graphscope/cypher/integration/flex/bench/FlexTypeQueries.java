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

package com.alibaba.graphscope.cypher.integration.flex.bench;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlexTypeQueries {
    private final Map<String, Parameter> queryParameters;

    private static class Parameter {
        private final String name;
        private final List<String> parameters;
        private final List<String> results;

        public Parameter(String line) {
            String[] parts = line.split("\\|");
            Preconditions.checkArgument(parts.length >= 3, "invalid parameter line: " + line);
            this.name = parts[0].trim();
            this.parameters =
                    List.of(parts[1].trim().split(",")).stream()
                            .map(k -> k.trim())
                            .collect(Collectors.toList());
            this.results =
                    List.of(parts[2].trim().split(",")).stream()
                            .map(k -> k.trim())
                            .collect(Collectors.toList());
        }

        public String render(String template) {
            for (int i = 1; i <= parameters.size(); i++) {
                template = template.replaceAll("\\$" + i, parameters.get(i - 1));
            }
            return template;
        }
    }

    public FlexTypeQueries(String inputPath) throws Exception {
        List<String> parameters = FileUtils.readLines(new File(inputPath), StandardCharsets.UTF_8);
        this.queryParameters = Maps.newHashMap();
        for (String parameter : parameters) {
            if (parameter.trim().isEmpty() || parameter.startsWith("//")) continue;
            Parameter param = new Parameter(parameter);
            this.queryParameters.put(param.name, param);
        }
    }

    public CompareTest getCompare() {
        return new CompareTest();
    }

    public PlusTest getPlus() {
        return new PlusTest();
    }

    public MinusTest getMinus() {
        return new MinusTest();
    }

    public MultiplyTest getMultiply() {
        return new MultiplyTest();
    }

    public DivideTest getDivide() {
        return new DivideTest();
    }

    // Assign type to the literal value with prefix or suffix characters.
    // i.e. 1 denote int32, +1 denote uint32, 1L denote int64, +1L denote uint64, 1.0d denote
    // double, 1.0f denote float.
    public class CompareTest {
        /**
         * Compare an int32 type property with an int32 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint32 type property with a uint32 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_uint32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_uint32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint32 type property with an int32 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_uint32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint32 type property with an int32 literal, where the int32 literal is negative
         * and has the same binary representation as the uint32 property value.
         *
         * Expected Result:
         *      The query should return empty due to integer overflow.
         * @return
         */
        public QueryContext compare_uint32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint64 type property with a uint64 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_uint64_uint64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint64 = $1\n"
                            + "    RETURN p.prop_uint64";
            Parameter parameter = queryParameters.get("compare_uint64_uint64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int64 type property with an int64 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_int64_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int64 = $1\n"
                            + "    RETURN p.prop_int64";
            Parameter parameter = queryParameters.get("compare_int64_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint64 type property with an int64 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_uint64_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint64 = $1\n"
                            + "    RETURN p.prop_uint64";
            Parameter parameter = queryParameters.get("compare_uint64_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a uint64 type property with an int64 literal, where the int64 literal is negative
         * and has the same binary representation as the uint64 property value.
         *
         * Expected Result:
         *      The query should return empty due to integer overflow.
         * @return
         */
        public QueryContext compare_uint64_int64_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint64 = $1\n"
                            + "    RETURN p.prop_uint64";
            Parameter parameter = queryParameters.get("compare_uint64_int64_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with an int64 literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with an int64 literal that exceeds the range of int32. The int64 after overflow has the same binary representation as the int32 value.
         *
         * Expected Result:
         *      The query should return empty due to overflow.
         * @return
         */
        public QueryContext compare_int32_int64_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int64_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a float literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_float_float_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_float");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a double type property with a double literal.
         *
         * Expected Result:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_double_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_double = $1\n"
                            + "    RETURN p.prop_double";
            Parameter parameter = queryParameters.get("compare_double_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a double literal.
         *
         * Expected Result:
         *      The query should return the float value as the result.
         * @return
         */
        public QueryContext compare_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a double literal, where the double literal has more precision
         * than the float property, leading to precision loss.
         *
         * Expected Result:
         *      The query should return empty due to precision loss.
         * @return
         */
        public QueryContext compare_float_double_loss_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_double_loss");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with a double literal.
         *
         * Expected Result:
         *      The query should return the property value as an int32.
         * @return
         */
        public QueryContext compare_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a characters type property with a string literal, the literal value is identical to the property value.
         *
         * Expected Results:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_char_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_char = $1\n"
                            + "    RETURN p.prop_char";
            Parameter parameter = queryParameters.get("compare_char_text");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a characters type property with a string literal, the literal is longer than the property value.
         *
         * Expected Results:
         *      The query should return the property value, the literal has been truncated to the length of the property value before comparison.
         * @return
         */
        public QueryContext compare_char_long_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_char = $1\n"
                            + "    RETURN p.prop_char";
            Parameter parameter = queryParameters.get("compare_char_long_text");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a varchar type property with a string literal, the literal is longer than the property value.
         *
         * Expected Results:
         *     The query should return the property value, the literal has been truncated to the length of the property value before comparison.
         * @return
         */
        public QueryContext compare_varchar_long_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_varchar = $1\n"
                            + "    RETURN p.prop_varchar";
            Parameter parameter = queryParameters.get("compare_varchar_long_text");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an unlimited length type property with a long text literal, the literal is identical to the property value.
         *
         * Expected Results:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_string_long_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_text = $1\n"
                            + "    RETURN p.prop_text";
            Parameter parameter = queryParameters.get("compare_string_long_text");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a date32 type property with an i32 literal.
         *
         * Expected Results:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_date32_i32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_date = $1\n"
                            + "    RETURN p.prop_date";
            Parameter parameter = queryParameters.get("compare_date32_i32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a timestamp type property with an i64 literal.
         *
         * Expected Results:
         *      The query should return the property value.
         * @return
         */
        public QueryContext compare_timestamp_i64_test() {
            String query =
                    "MATCH (p:person)\n" + "    WHERE p.prop_ts = $1\n" + "    RETURN p.prop_ts";
            Parameter parameter = queryParameters.get("compare_timestamp_i64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }
    }

    public class PlusTest {

        // plus_int32_int32|12,13|25
        /**
         * Plus an int32 value with an int32 value, the sum is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_int32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_int32_overflow|2147483647,1|overflow
        /**
         * Plus an int32 value with an int32 value, the sum exceeds the range of int32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int32 overflow.
         */
        public QueryContext plus_int32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_uint32_int32|12,+13|25
        /**
         * Plus an int32 value with an uint32 value, the sum is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_int32_uint32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_uint32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_uint32_uint32|10,+2147483647|+2147483657
        /**
         * Plus an int32 value with an uint32 value, the sum is out of range of int32 but within the range of uint32.
         *
         * Expected Results:
         *      The query should return a uint32 type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_int32_uint32_uint32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_uint32_uint32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_uint32_overflow|1,+4294967295|overflow
        /**
         * Plus an int32 value with an uint32 value, the sum exceeds the range of uint32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to uint32 overflow.
         */
        public QueryContext plus_int32_uint32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_uint32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_int64|12,14L|26L
        /**
         * Plus an int32 value with an int64 value, the sum is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_int32_int64_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // plus_int32_int64_overflow|10,9223372036854775807L|overflow
        /**
         * Plus an int32 value with an int64 value, the sum exceeds the range of int64.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int64 overflow.
         */
        public QueryContext plus_int32_int64_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int64_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 value with a double value.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_int32_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_int32_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus a float type value with a double value.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the sum of $1 and $2.
         */
        public QueryContext plus_float_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 + $2";
            Parameter parameter = queryParameters.get("plus_float_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }
    }

    public class MinusTest {

        // minus_int32_int32|12,13|-1
        /**
         * Minus an int32 value with an int32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_int32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_int32_int32_overflow|2147483647,-1|overflow
        /**
         * Minus an int32 value with an int32 value, the result exceeds the range of int32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int32 overflow.
         */
        public QueryContext minus_int32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_int32_uint32_int32|12,+13|-1
        /**
         * Minus an int32 value with an uint32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_int32_uint32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int32_uint32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_uint32_int32_uint32|+2147483647,-10|+2147483657
        /**
         * Minus an uint32 value with an int32 value, the result is within the range of uint32.
         *
         * Expected Results:
         *      The query should return an uint32 type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_uint32_int32_uint32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_uint32_int32_uint32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_uint32_int32_overflow|+4294967295,-1|overflow
        /**
         * Minus an uint32 value with an int32 value, the result exceeds the range of uint32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to uint32 overflow.
         */
        public QueryContext minus_uint32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_uint32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_int32_int64|12,13L|-1L
        /**
         * Minus an int32 value with an int64 value, the result is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_int32_int64_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_int64_int32_overflow|9223372036854775807L,-10|overflow
        /**
         * Minus an int64 value with an int32 value, the result exceeds the range of int64.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int64 overflow.
         */
        public QueryContext minus_int64_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int64_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_int32_double|12,13.12d|-1.12d
        /**
         * Minus an int32 value with a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_int32_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_int32_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // minus_float_double|12.0f,13.12d|-1.12d
        /**
         * Minus a float value with a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the difference of $1 and $2.
         */
        public QueryContext minus_float_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 - $2";
            Parameter parameter = queryParameters.get("minus_float_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }
    }

    public class MultiplyTest {

        // multiply_int32_int32|1,2|2
        /**
         * Multiply an int32 value with an int32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_int32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_int32_overflow|2147483647,2|overflow
        /**
         * Multiply an int32 value with an int32 value, the result exceeds the range of int32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int32 overflow.
         */
        public QueryContext multiply_int32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_uint32_int32|1,+2|2
        /**
         * Multiply an int32 value with an uint32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_int32_uint32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_uint32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_uint32_uint32|4,+1000000000|+4000000000
        /**
         * Multiply an int32 value with an uint32 value, the result is out of the range of int32 but within the range of uint32.
         *
         * Expected Results:
         *      The query should return a uint32 type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_int32_uint32_uint32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_uint32_uint32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_uint32_overflow|4,+2000000000|overflow
        /**
         * Multiply an int32 value with an uint32 value, the result exceeds the range of uint32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to uint32 overflow.
         */
        public QueryContext multiply_int32_uint32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_uint32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_int64|1,2L|2L
        /**
         * Multiply an int32 value with an int64 value, the result is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_int32_int64_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_int64_overflow|2,9223372036854775807L|overflow
        /**
         * Multiply an int32 value with an int64 value, the result exceeds the range of int64.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int64 overflow.
         */
        public QueryContext multiply_int32_int64_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int64_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_int32_double|2,2.12d|4.24d
        /**
         * Multiply an int32 value with a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_int32_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // multiply_float_double|2.0f,2.12d|4.24d
        /**
         * Multiply a float value with a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the product of $1 and $2.
         */
        public QueryContext multiply_float_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 * $2";
            Parameter parameter = queryParameters.get("multiply_float_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }
    }

    public class DivideTest {

        // divide_int32_int32|2,1|2
        /**
         * Divide an int32 value by an int32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_int32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_int32_int32_overflow|-2147483648,-1|overflow
        /**
         * Divide an int32 value by an int32 value, the result exceeds the range of int32.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int32 overflow.
         */
        public QueryContext divide_int32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_int32_uint32_int32|2,+1|2
        /**
         * Divide an int32 value by an uint32 value, the result is within the range of int32.
         *
         * Expected Results:
         *      The query should return an int32 type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_int32_uint32_int32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int32_uint32_int32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_uint32_int32_uint32|+4294967295,1|+4294967295
        /**
         * Divide an uint32 value by an int32 value, the result is out of the range of int32 but within the range of uint32.
         *
         * Expected Results:
         *      The query should return a uint32 type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_uint32_int32_uint32_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_uint32_int32_uint32");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_uint32_int32_overflow|+4294967295,-1|overflow
        /**
         * Divide an uint32 value by a negative int32 value, the result exceeds the range of int32 (INT32_MIN).
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int32 overflow.
         */
        public QueryContext divide_uint32_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_uint32_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_int32_int64|2,1L|2L
        /**
         * Divide an int32 value by an int64 value, the result is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_int32_int64_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int64");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_int64_int32_overflow|-9223372036854775808L,-1|overflow
        /**
         * Divide an int64 value by an int32 value, the result exceeds the range of int64.
         *
         * Expected Results:
         *      The query should throw an overflow exception due to int64 overflow.
         */
        public QueryContext divide_int64_int32_overflow_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int64_int32_overflow");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_int32_double|2,1.12d|1.77d
        /**
         * Divide an int32 value by a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_int32_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_int32_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_float_double|2.0f,1.12d|1.78d
        /**
         * Divide a float value by a double value, the result is within the range of double.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the quotient of $1 and $2.
         */
        public QueryContext divide_float_double_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_float_double");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }

        // divide_float_double_NaN|2.0f,0.0d|NaN
        /**
         * Divide a float value by a double value, where the divisor is zero, resulting in NaN.
         *
         * Expected Results:
         *      The query should return NaN as the result of division by zero.
         */
        public QueryContext divide_float_double_NaN_test() {
            String query = "MATCH (p:person {prop_int32: 933})\n" + "    RETURN $1 / $2";
            Parameter parameter = queryParameters.get("divide_float_double_NaN");
            query = parameter.render(query);
            return new QueryContext(query, parameter.results);
        }
    }

    /**
     * Find 'knows' edges between two persons of specific property values.
     *
     * Expected Results:
     *      The query should return the property values for the two persons involved in the 'knows' relationship.
     * @return
     */
    public QueryContext get_knows_between_two_persons_test() {
        String query =
                "MATCH (p1:person)-[r:knows]->(p2:person)\n"
                        + "    WHERE p1.prop_int32 = $1 AND p2.prop_int32 = $2\n"
                        + "    RETURN p1.prop_int32, p2.prop_int32";
        Parameter parameter = queryParameters.get("get_knows_between_two_persons");
        // render the query, replace the $1 and $2 with the actual values
        for (int i = 0; i < parameter.parameters.size(); i++) {
            query = query.replace("$" + (i + 1), parameter.parameters.get(i));
        }
        return new QueryContext(query, parameter.results);
    }
}
