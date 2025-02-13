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

package com.alibaba.graphscope.cypher.integration.suite;

import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class FlexTypeQueries {
    private final Map<String, Parameter> queryParameters;

    private static class Parameter {
        private final String name;
        private final List<String> parameters;
        private final List<String> results;

        public Parameter(String config) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    public FlexTypeQueries(String inputPath) throws Exception {
        List<String> parameters = FileUtils.readLines(new File(inputPath), StandardCharsets.UTF_8);
        this.queryParameters = Maps.newHashMap();
        for (String parameter : parameters) {
            Parameter param = new Parameter(parameter);
            this.queryParameters.put(param.name, param);
        }
    }

    private class CompareQueries {
        /**
         * Compare an int32 type property with an int32 literal.
         *
         * Expected Result:
         *      The query should return the value of $1.
         * @return
         */
        public QueryContext compare_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint32 type property with an uint32 literal.
         *
         * Expected Results:
         *    The query should return the value of $1.
         * @return
         */
        public QueryContext compare_uint32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_uint32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint32 type property with an int32 literal, the literal value is positive and within the range of uint32.
         *
         * Expected Results:
         *   The query should return the property value.
         * @return
         */
        public QueryContext compare_uint32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint32 type property with an int32 literal, the literal value is negative and has the same binary representation as the property value.
         *
         * Expected Results:
         *      The query should return empty due to integer overflow.
         * @return
         */
        public QueryContext compare_uint32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32";
            Parameter parameter = queryParameters.get("compare_uint32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint64 type property with an uint64 literal.
         *
         * Expected Results:
         *      The query should return the value of $1.
         * @return
         */
        public QueryContext compare_uint64_uint64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint64 = $1\n"
                            + "    RETURN p.prop_uint64";
            Parameter parameter = queryParameters.get("compare_uint64_uint64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int64 type property with an int64 literal.
         *
         * Expected Results:
         *      The query should return the value of $1.
         * @return
         */
        public QueryContext compare_int64_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int64 = $1\n"
                            + "    RETURN p.prop_int64";
            Parameter parameter = queryParameters.get("compare_int64_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint64 type property with an int64 literal, the literal value is positive and within the range of uint64.
         *
         * Expected Results:
         *      The query should return the value of $1.
         */
        public QueryContext compare_uint64_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint64 = $1\n"
                            + "    RETURN p.prop_uint64";
            Parameter parameter = queryParameters.get("compare_uint64_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an uint64 type property with an int64 literal, the literal value is negative and has the same binary representation as the property value.
         *
         * Expected Results:
         *      The query should return empty due to integer overflow.
         * @return
         */
        public QueryContext compare_uint64_int64_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int64 = $1\n"
                            + "    RETURN p.prop_int64";
            Parameter parameter = queryParameters.get("compare_uint64_int64_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with an int64 literal, the int64 value is within the range of int32.
         *
         * Expected Results:
         *     The query should return the value of $1.
         * @return
         */
        public QueryContext compare_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with an int64 literal, the literal value is out the range of int32, but the lowest 32-bits are the same with the property value.
         *
         * Expected Results:
         *     The query should return empty due to integer overflow.
         * @return
         */
        public QueryContext compare_int32_int64_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_int64_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a float literal.
         *
         * Expected Results:
         *      The query should return the value of $1.
         * @return
         */
        public QueryContext compare_float_float_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_float");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a double type property with a double literal.
         *
         * Expected Results:
         *      The query should return the value of $1.
         */
        public QueryContext compare_double_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_double = $1\n"
                            + "    RETURN p.prop_double";
            Parameter parameter = queryParameters.get("compare_double_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a double literal, they are identical in double representation, like 1.2f and 1.20d.
         *
         * Expected Results:
         *      The query should return the property value.
         *
         * @return
         */
        public QueryContext compare_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a float type property with a double literal, they are not equal due to precision loss, like 1.2f and 1.23d.
         *
         * Expected Results:
         *      The query should return empty.
         */
        public QueryContext compare_float_double_loss_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float";
            Parameter parameter = queryParameters.get("compare_float_double_loss");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare an int32 type property with a double literal, they are identical in double representation, like 1 and 1.0d
         *
         * Expected Results:
         *      The query should return the property value
         * @return
         */
        public QueryContext compare_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32";
            Parameter parameter = queryParameters.get("compare_int32_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
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
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a characters type property with a string literal, the literal is longer than the property value.
         *
         * Expected Results:
         *      The query should return the property value, the literal is truncated to the length of the property value before comparison.
         * @return
         */
        public QueryContext compare_char_long_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_char = $1\n"
                            + "    RETURN p.prop_char";
            Parameter parameter = queryParameters.get("compare_char_long_text");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a varchar type property with a string literal, the literal is longer than the property value.
         *
         * Expected Results:
         *     The query should return the property value, the literal is truncated to the length of the property value before comparison.
         * @return
         */
        public QueryContext compare_varchar_long_text_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_varchar = $1\n"
                            + "    RETURN p.prop_varchar";
            Parameter parameter = queryParameters.get("compare_varchar_long_text");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Compare a string type (unlimited length) with a long text literal, the literal is identical to the property value.
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
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
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
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
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
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }
    }

    public class PlusTest {
        /**
         * Plus an int32 type property with an int32 literal, the sum is within the range of int32.
         *
         * Expected Results:
         *      The query should return the sum result of property and $2
         *
         * @return
         */
        public QueryContext plus_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 type property with an int32 literal, the sum is out of the range of int32.
         *
         * Expected Results:
         *      The query should throw an exception or return a wrong result due to overflow.
         */
        public QueryContext plus_int32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 type property with an uint32 literal, the property value is positive and the sum is within the range of uint32.
         *
         * Expected Results:
         *    The query should return an uint32 type value, identical to the sum of property and $2.
         */
        public QueryContext plus_int32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_uint32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 type property with an uint32 literal, the property value is negative.
         *
         * Expected Results:
         *      The query should throw an exception or return a wrong result due to overflow.
         *
         * NOTICE to differentiate with the 'plus_int32_int32_test', even though they have the same parameters.
         * For example, signed(-100) + signed(100) = 0, but signed(-100) + unsigned(100) = 4294967296, out of the maximum value of UINT32_MAX(4294967295).
         *
         */
        public QueryContext plus_int32_uint32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_uint32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 type property with an int64 literal, the sum is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the sum of property and $2
         * @return
         */
        public QueryContext plus_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus an int32 type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the sum of property and $2.
         */
        public QueryContext plus_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 + $2";
            Parameter parameter = queryParameters.get("plus_int32_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Plus a float type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the sum of property and $2.
         */
        public QueryContext plus_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float + $2";
            Parameter parameter = queryParameters.get("plus_float_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }
    }

    public class MinusTest {
        /**
         * Minus an int32 type property with an int32 literal, the minus is within the range of int32.
         *
         * Expected Results:
         *      The query should return the minus result of property and $2
         *
         * @return
         */
        public QueryContext minus_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus an int32 type property with an int32 literal, the minus is out of the range of int32.
         *
         * Expected Results:
         *      The query should throw an exception or return a wrong result due to overflow.
         */
        public QueryContext minus_int32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus an int32 type property with an uint32 literal, the property value is positive and the minus is within the range of uint32.
         *
         * Expected Results:
         *    The query should return an uint32 type value, identical to the minus of property and $2.
         */
        public QueryContext minus_int32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_uint32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus an int32 type property with an uint32 literal, the minus is negative, which is out of the range of uint32.
         *
         * Expected Results:
         *      The query should throw an exception or return a wrong result due to overflow.
         *
         */
        public QueryContext minus_int32_uint32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_uint32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus an int32 type property with an int64 literal, the minus is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the minus of property and $2
         * @return
         */
        public QueryContext minus_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus an int32 type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the minus of property and $2.
         */
        public QueryContext minus_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 - $2";
            Parameter parameter = queryParameters.get("minus_int32_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Minus a float type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the minus of property and $2.
         */
        public QueryContext minus_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float - $2";
            Parameter parameter = queryParameters.get("minus_float_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }
    }

    public class MultiplyTest {
        /**
         * Multiply an int32 type property with an int32 literal, the product is within the range of int32.
         *
         * Expected Results:
         *      The query should return the product result of property and $2
         *
         * @return
         */
        public QueryContext multiply_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply an int32 type property with an int32 literal, the product is out of the range of int32.
         *
         * Expected Results:
         *      The query should throw an exception or return a wrong result due to overflow.
         */
        public QueryContext multiply_int32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply an int32 type property with an uint32 literal, the property value is positive and the product is within the range of uint32.
         *
         * Expected Results:
         *    The query should return an uint32 type value, identical to the product of property and $2.
         */
        public QueryContext multiply_int32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_uint32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply an int32 type property with an uint32 literal, the property value is negative.
         *
         * Expected Results:
         *   The query should throw an exception or return a wrong result due to overflow, i.e. signed(-100) * unsigned(100) is out of uint32 range.
         * @return
         */
        public QueryContext multiply_int32_uint32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_uint32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply an int32 type property with an int64 literal, the product is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the product of property and $2
         * @return
         */
        public QueryContext multiply_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply an int32 type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the product of property and $2.
         */
        public QueryContext multiply_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 * $2";
            Parameter parameter = queryParameters.get("multiply_int32_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Multiply a float type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the product of property and $2.
         */
        public QueryContext multiply_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float * $2";
            Parameter parameter = queryParameters.get("multiply_float_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }
    }

    public class DivideTest {
        /**
         * Divide an int32 type property with an int32 literal, the division is within the range of int32.
         *
         * Expected Results:
         * The query should return the division result of property and $2
         *
         * @return
         */
        public QueryContext divide_int32_int32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide an int32 type property with an int32 literal, the division is out of the maximum value of int32.
         * <p>
         * Expected Results:
         * The query should throw an exception or return a wrong result due to overflow.
         */
        public QueryContext divide_int32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide an int32 type property with an uint32 literal, the division is within the range of uint32.
         *
         * Expected Results:
         * The query should return an uint32 type value, identical to the division of property and $2.
         */
        public QueryContext divide_int32_uint32_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 / $2";
            Parameter parameter = queryParameters.get("divide_int32_uint32");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide an uint32 type property with an int32 literal, the literal value is negative.
         *
         * Expected Results:
         *   The query should throw an exception or return a wrong result due to overflow.
         * @return
         */
        public QueryContext divide_uint32_int32_overflow_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_uint32 = $1\n"
                            + "    RETURN p.prop_uint32 / $2";
            Parameter parameter = queryParameters.get("divide_uint32_int32_overflow");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide an int32 type property with an int64 literal, the division is within the range of int64.
         *
         * Expected Results:
         *      The query should return an int64 type value, identical to the division of property and $2
         * @return
         */
        public QueryContext divide_int32_int64_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 / $2";
            Parameter parameter = queryParameters.get("divide_int32_int64");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide an int32 type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the division of property and $2.
         */
        public QueryContext divide_int32_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_int32 = $1\n"
                            + "    RETURN p.prop_int32 / $2";
            Parameter parameter = queryParameters.get("divide_int32_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide a float type property with a double literal.
         *
         * Expected Results:
         *      The query should return a double type value, identical to the division of property and $2.
         */
        public QueryContext divide_float_double_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float / $2";
            Parameter parameter = queryParameters.get("divide_float_double");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
            return new QueryContext(query, parameter.results);
        }

        /**
         * Divide a float type property with 0.0d.
         *
         * Expected Results:
         *    The query should throw NaN errors.
         */
        public QueryContext divide_float_double_NaN_test() {
            String query =
                    "MATCH (p:person)\n"
                            + "    WHERE p.prop_float = $1\n"
                            + "    RETURN p.prop_float / $2";
            Parameter parameter = queryParameters.get("divide_float_double_NaN");
            // render the query, replace the $1 with the actual value
            for (int i = 0; i < parameter.parameters.size(); i++) {
                query = query.replace("$" + (i + 1), parameter.parameters.get(i));
            }
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
