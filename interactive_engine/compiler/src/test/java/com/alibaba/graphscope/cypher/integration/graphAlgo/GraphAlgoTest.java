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

package com.alibaba.graphscope.cypher.integration.graphAlgo;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.alibaba.graphscope.cypher.integration.suite.graphAlgo.GraphAlgoQueries;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class GraphAlgoTest {

    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_graph_query1_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test1();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query2_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test2();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query3_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test3();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query4_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test4();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query5_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test5();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query6_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test6();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query7_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test7();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query8_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test8();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query9_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test9();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query10_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test10();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query11_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test11();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query12_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test12();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_graph_query13_test() {
        QueryContext testQuery = GraphAlgoQueries.get_graph_algo_test13();
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
