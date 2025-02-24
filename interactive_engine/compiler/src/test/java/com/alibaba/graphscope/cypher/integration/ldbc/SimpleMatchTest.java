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

import static org.junit.Assume.assumeTrue;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.alibaba.graphscope.cypher.integration.suite.simple.SimpleMatchQueries;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleMatchTest {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMatchTest.class);

    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_simple_match_1_test() {
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
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
        // TODO: fix this in hiactor.
        assumeTrue("pegasus".equals(System.getenv("ENGINE_TYPE")));
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

    @Test
    public void run_simple_match_17_test() {
        assumeTrue("pegasus".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_17_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_18_test() {
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_18_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_simple_match_19_test() {
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_19_test();
        Result result = session.run(testQuery.getQuery());
        List<Record> records = result.list();
        List<String> properties = new ArrayList<>();
        records.forEach(
                record -> {
                    properties.add(fetchAllProperties(record));
                });
        logger.info(properties.toString());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), properties.toString());
    }

    @Test
    public void run_simple_match_20_test() {
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = SimpleMatchQueries.get_simple_match_query_20_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    private static String fetchAllProperties(Record record) {
        List<String> properties = new ArrayList<>();
        record.keys()
                .forEach(
                        key -> {
                            Value v = record.get(key);
                            if (v.hasType(InternalTypeSystem.TYPE_SYSTEM.NODE())) {
                                Node node = v.asNode();
                                Map<String, Object> nodeProperties = node.asMap();
                                properties.add(key + ": " + nodeProperties.toString());
                            } else {
                                properties.add(key + ": " + record.get(key).toString());
                            }
                        });
        return properties.toString();
    }

    @AfterClass
    public static void afterClass() {
        if (session != null) {
            session.close();
        }
    }
}
