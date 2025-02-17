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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;

public class FlexTypeTest {
    private static final Logger logger = LoggerFactory.getLogger(FlexTypeTest.class);
    private static Session session;

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
        runComponent(queries.getCompare());

        // run arithmetic plus test
        runComponent(queries.getPlus());

        // run arithmetic minus test
        runComponent(queries.getMinus());

        // run arithmetic multiply test
        runComponent(queries.getMultiply());

        // run arithmetic divide test
        runComponent(queries.getDivide());
    }

    private void runComponent(Object component) throws Exception {
        Method[] methods = component.getClass().getDeclaredMethods();

        for (Method method : methods) {
            logger.warn(method.getName() + " is running");
            QueryContext ctx = (QueryContext) method.invoke(component);
            checkResult(() -> session.run(ctx.getQuery()), ctx);
            logger.warn(method.getName() + " is done");
        }
    }

    private void checkResult(Supplier<Result> resultSupplier, QueryContext ctx) {
        List<String> expected = ctx.getExpectedResult();
        if (expected.size() == 1 && expected.get(0).equals("empty")) {
            Assert.assertTrue(resultSupplier.get().list().isEmpty());
        } else if (expected.size() == 1 && expected.get(0).equals("overflow")) {
            try {
                resultSupplier.get().list();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("overflow"));
            }
        } else if (expected.size() == 1 && expected.get(0).equals("NaN")) {
            try {
                resultSupplier.get().list();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("NaN"));
            }
        } else {
            Record single = resultSupplier.get().list().get(0);
            Assert.assertEquals(expected.size(), single.size());
            for (int i = 0; i < expected.size(); i++) {
                Value actual = single.get(i);
                String expectedValue = expected.get(i);
                if (expectedValue.startsWith("+")) {
                    expectedValue = expectedValue.substring(1);
                }
                String upperCase = expectedValue.toUpperCase();
                if (upperCase.endsWith("L") || upperCase.endsWith("D") || upperCase.endsWith("F")) {
                    expectedValue = expectedValue.substring(0, expectedValue.length() - 1);
                }
                Assert.assertEquals(expectedValue, actual.asString());
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        session.close();
    }
}
