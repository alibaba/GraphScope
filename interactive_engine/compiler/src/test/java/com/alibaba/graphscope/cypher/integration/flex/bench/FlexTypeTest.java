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

import com.alibaba.graphscope.cypher.antlr4.Utils;
import com.alibaba.graphscope.cypher.integration.suite.QueryContext;

import org.javatuples.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

public class FlexTypeTest {
    private static final Logger logger = LoggerFactory.getLogger(FlexTypeTest.class);
    private static Session session;

    /**
     * start compiler before the test:
     *   make run graph.schema=./src/test/resources/flex_bench/modern.yaml graph.planner.opt=CBO graph.physical.opt=proto disable.expr.simplify=true
     */
    @BeforeClass
    public static void setUp() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run() throws Exception {
        FlexTypeQueries queries = new FlexTypeQueries("src/test/resources/flex_bench/parameters");

        // run comparison test, comparing numeric, string and temporal types
        Pair<Integer, Integer> compare = runComponent(queries.getCompare());

        // run arithmetic plus test
        Pair<Integer, Integer> plus = runComponent(queries.getPlus());

        // run arithmetic minus test
        Pair<Integer, Integer> minus = runComponent(queries.getMinus());

        // run arithmetic multiply test
        Pair<Integer, Integer> multi = runComponent(queries.getMultiply());

        // run arithmetic divide test
        Pair<Integer, Integer> divide = runComponent(queries.getDivide());

        logger.warn(
                "\n component: Compare, total tests {}, passed tests {}"
                        + "\n component: Plus, total tests {}, passed tests {}"
                        + "\n component: Minus, total tests {}, passed tests {}"
                        + "\n component: Multiply, total tests {}, passed tests {}"
                        + "\n component: Divide, total tests {}, passed tests {}",
                compare.getValue0(),
                compare.getValue1(),
                plus.getValue0(),
                plus.getValue1(),
                minus.getValue0(),
                minus.getValue1(),
                multi.getValue0(),
                multi.getValue1(),
                divide.getValue0(),
                divide.getValue1());

        int total =
                compare.getValue0()
                        + plus.getValue0()
                        + minus.getValue0()
                        + multi.getValue0()
                        + divide.getValue0();
        int passed =
                compare.getValue1()
                        + plus.getValue1()
                        + minus.getValue1()
                        + multi.getValue1()
                        + divide.getValue1();
        Assert.assertEquals("total tests: " + total + ", passed tests: " + passed, total, passed);
    }

    private Pair<Integer, Integer> runComponent(Object component) {
        Method[] methods = component.getClass().getDeclaredMethods();
        int totalTests = methods.length;
        int passTests = 0;
        for (Method method : methods) {
            try {
                QueryContext ctx = (QueryContext) method.invoke(component);
                checkResult(() -> session.run(ctx.getQuery()), ctx);
                logger.warn("test {} passed.", method.getName());
                ++passTests;
            } catch (Throwable t) {
                logger.error("test {} failed.", method.getName(), t);
            }
        }
        return Pair.with(totalTests, passTests);
    }

    private void checkResult(Supplier<Result> resultSupplier, QueryContext ctx) {
        List<String> expected = ctx.getExpectedResult();
        if (expected.size() == 1 && expected.get(0).equals("empty")) {
            Assert.assertTrue(resultSupplier.get().list().isEmpty());
        } else if (expected.size() == 1 && expected.get(0).equals("overflow")) {
            try {
                resultSupplier.get().list();
                Assert.fail("overflow exception should have been thrown");
            } catch (Exception e) {
                Assert.assertTrue(
                        "cannot catch overflow exception from execution message",
                        e.getMessage().contains("overflow"));
            }
        } else if (expected.size() == 1 && expected.get(0).equals("NaN")) {
            try {
                resultSupplier.get().list();
                Assert.fail("NaN exception should have been thrown");
            } catch (Exception e) {
                Assert.assertTrue(
                        "cannot catch NaN exception from execution message",
                        e.getMessage().contains("NaN"));
            }
        } else {
            List<Record> records = resultSupplier.get().list();
            Assert.assertTrue("records should not be empty", !records.isEmpty());
            Record single = records.get(0);
            Assert.assertEquals(expected.size(), single.size());
            for (int i = 0; i < expected.size(); i++) {
                Value actual = single.get(i);
                String expectedValue = expected.get(i);
                boolean unsigned = false;
                if (expectedValue.startsWith("+")) {
                    expectedValue = expectedValue.substring(1);
                    unsigned = true;
                }
                boolean int32 = true;
                if (expectedValue.toLowerCase().endsWith("l")) {
                    int32 = false;
                }
                String upperCase = expectedValue.toUpperCase();
                if (upperCase.endsWith("L") || upperCase.endsWith("D") || upperCase.endsWith("F")) {
                    expectedValue = expectedValue.substring(0, expectedValue.length() - 1);
                }
                String actualValue = getActualValue(actual, unsigned, int32);
                Assert.assertEquals(expectedValue, actualValue);
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        session.close();
    }

    public String getActualValue(Value actual, boolean unsigned, boolean int32) {
        if (unsigned) {
            if (int32) {
                int value = actual.asInt();
                return new BigDecimal(new BigInteger(1, Utils.intToBytes(value))).toString();
            } else {
                long value = actual.asLong();
                return new BigDecimal(new BigInteger(1, Utils.longToBytes(value))).toString();
            }
        }
        return actual.toString();
    }
}
