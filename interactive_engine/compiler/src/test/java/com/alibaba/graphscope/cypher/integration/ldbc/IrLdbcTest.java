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

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.alibaba.graphscope.cypher.integration.suite.ldbc.LdbcQueries;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class IrLdbcTest {
    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_ldbc_2_test() {
        QueryContext testQuery = LdbcQueries.get_ldbc_2_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_3_test() {
        QueryContext testQuery = LdbcQueries.get_ldbc_3_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_4_test() {
        // skip this test in pegasus (actually exp-store) since the date format is different.
        assumeFalse("pegasus".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_4_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_4_test_exp() {
        assumeTrue("pegasus".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_4_test_exp();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_6_test() {
        QueryContext testQuery = LdbcQueries.get_ldbc_6_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_7_test() {
        // skip this test in pegasus as optional match (via optional edge_expand) is not supported
        // yet.
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_7_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_8_test() {
        assumeFalse("pegasus".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_8_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_8_test_exp() {
        assumeTrue("pegasus".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_8_test_exp();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_10_test() {
        // skip this test in pegasus (actually exp-store and groot-store) since the date format is
        // different
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_ldbc_10_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_ldbc_12_test() {
        QueryContext testQuery = LdbcQueries.get_ldbc_12();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_st_path_test() {
        // skip st path test in gs interactive for it is unsupported yet in hiactor engine
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = LdbcQueries.get_st_path();
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
