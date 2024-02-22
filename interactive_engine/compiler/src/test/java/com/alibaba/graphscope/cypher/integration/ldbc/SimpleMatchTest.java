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
package com.alibaba.graphscope.cypher.integration.ldbc;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.alibaba.graphscope.cypher.integration.suite.simple.SimpleMatchQueries;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class SimpleMatchTest {

    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_simple_match_1_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_1_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_2_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_2_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_3_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_3_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_4_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_4_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_5_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_5_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_6_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_6_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_7_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_7_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_8_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_8_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_9_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_9_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_10_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_10_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_11_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_11_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_12_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_12_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_13_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_13_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_14_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_14_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_15_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_15_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_16_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_16_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @AfterClass
    public static void afterClass() {
        if (session != null) {
            session.close();
        }
    }
}
