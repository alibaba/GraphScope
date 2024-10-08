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

package com.alibaba.graphscope.cypher.integration.movie;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.alibaba.graphscope.cypher.integration.suite.movie.MovieQueries;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class MovieTest {

    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_movie_query1_test() {
        QueryContext testQuery = MovieQueries.get_movie_query1_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query2_test() {
        QueryContext testQuery = MovieQueries.get_movie_query2_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query3_test() {
        QueryContext testQuery = MovieQueries.get_movie_query3_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query4_test() {
        QueryContext testQuery = MovieQueries.get_movie_query4_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query5_test() {
        QueryContext testQuery = MovieQueries.get_movie_query5_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query6_test() {
        QueryContext testQuery = MovieQueries.get_movie_query6_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query7_test() {
        QueryContext testQuery = MovieQueries.get_movie_query7_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query11_test() {
        QueryContext testQuery = MovieQueries.get_movie_query11_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query12_test() {
        QueryContext testQuery = MovieQueries.get_movie_query12_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query13_test() {
        QueryContext testQuery = MovieQueries.get_movie_query13_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query14_test() {
        QueryContext testQuery = MovieQueries.get_movie_query14_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query15_test() {
        QueryContext testQuery = MovieQueries.get_movie_query15_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query16_test() {
        QueryContext testQuery = MovieQueries.get_movie_query16_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query17_test() {
        QueryContext testQuery = MovieQueries.get_movie_query17_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query18_test() {
        QueryContext testQuery = MovieQueries.get_movie_query18_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query19_test() {
        QueryContext testQuery = MovieQueries.get_movie_query19_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query20_test() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = MovieQueries.get_movie_query20_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query21_test() {
        QueryContext testQuery = MovieQueries.get_movie_query21_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query22_test() {
        assumeTrue("hiactor".equals(System.getenv("ENGINE_TYPE")));
        QueryContext testQuery = MovieQueries.get_movie_query22_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query23_test() {
        QueryContext testQuery = MovieQueries.get_movie_query23_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query24_test() {
        QueryContext testQuery = MovieQueries.get_movie_query24_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query25_test() {
        QueryContext testQuery = MovieQueries.get_movie_query25_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query26_test() {
        QueryContext testQuery = MovieQueries.get_movie_query26_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query27_test() {
        QueryContext testQuery = MovieQueries.get_movie_query27_test();
        Result result = session.run(testQuery.getQuery());
        Assert.assertEquals(testQuery.getExpectedResult().toString(), result.list().toString());
    }

    @Test
    public void run_movie_query28_test() {
        QueryContext testQuery = MovieQueries.get_movie_query28_test();
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
