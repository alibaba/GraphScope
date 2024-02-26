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

package com.alibaba.graphscope.cypher.integration.suite.graphAlgo;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;

import java.util.Arrays;
import java.util.List;

public class GraphAlgoQueries {

    public static QueryContext get_graph_algo_test1() {
        String query =
                "MATCH (p:Paper)-[:WorkOn]->(:Task)-[:Belong]->(ca:Topic)\n"
                        + "WITH distinct ca, count(p) as paperCount\n"
                        + "RETURN ca.category, paperCount\n "
                        + "ORDER BY paperCount desc;";
        List<String> expected =
                Arrays.asList(
                        "Record<{category: \"Pattern Matching\", paperCount: 30}>",
                        "Record<{category: \"Traversal\", paperCount:" + " 29}>",
                        "Record<{category: \"Centrality\", paperCount: 18}>",
                        "Record<{category: \"Covering\", paperCount: 14}>",
                        "Record<{category: \"Community Detection\", paperCount: 13}>",
                        "Record<{category: \"Cohesive Subgraph\", paperCount: 12}>",
                        "Record<{category: \"Similarity\", paperCount: 7}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test2() {
        String query =
                "MATCH (ca:Topic)<-[:Belong]-(a:Task),\n"
                        + "(a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),\n"
                        + "(s)-[:ApplyOn]->(ch:Challenge)\n"
                        + "WITH ca, ch, count(p) AS num \n"
                        + "RETURN ca.category AS category, ch.challenge AS challenge, num\n"
                        + "ORDER BY num DESC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{category: \"Pattern Matching\", challenge:"
                                + " \"Communication Overhead\", num: 22}>",
                        "Record<{category: \"Pattern Matching\", challenge:"
                                + " \"Load Balance\", num: 16}>",
                        "Record<{category: \"Traversal\", challenge:"
                                + " \"Communication Overhead\", num: 13}>",
                        "Record<{category: \"Traversal\", challenge: \"Parallelism"
                                + "\", num: 12}>",
                        "Record<{category: \"Centrality\", challenge: \"Parallelism\", num:"
                                + " 11}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test3() {
        String query =
                "MATCH(c: CCFField)<-[e]-(p:Paper) return type(e) As edgeType ORDER BY edgeType ASC"
                        + " LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test4() {
        String query = "MATCH(v)-[e]-() return distinct type(e) AS edgeType order by edgeType ASC;";
        List<String> expected =
                Arrays.asList(
                        "Record<{edgeType: \"WorkOn\"}>",
                        "Record<{edgeType: \"Resolve\"}>",
                        "Record<{edgeType: \"Target\"}>",
                        "Record<{edgeType: \"Belong\"}>",
                        "Record<{edgeType: \"Use\"}>",
                        "Record<{edgeType: \"ApplyOn\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"Citation\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test5() {
        String query =
                "MATCH(c: CCFField)<-[e]-(p:Paper) where type(e) = 'HasField' return type(e) As"
                        + " edgeType ORDER BY edgeType ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>",
                        "Record<{edgeType: \"HasField\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test6() {
        String query =
                "MATCH (p:Paper)-[:Resolve]->(ch:Challenge),\n"
                        + "(p1:Paper)-[:Resolve]->(ch),\n"
                        + "(p)-[c:Citation]->(p1) \n"
                        + "RETURN COUNT(distinct c) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 66}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test7() {

        String query =
                "MATCH (p:Paper)-[:Resolve]->(ch:Challenge),\n"
                    + "      (p1:Paper)-[:Resolve]->(ch),\n"
                    + "       (p)-[c:Citation]->(p1)\n"
                    + "WITH DISTINCT p, c, p1\n"
                    + "WITH p.id As paperId, p1.id As citationId\n"
                    + "RETURN paperId, citationId ORDER BY paperId ASC, citationId ASC LIMIT 5;";

        List<String> expected =
                Arrays.asList(
                        "Record<{paperId: 0, citationId: 3}>",
                        "Record<{paperId: 0, citationId: 4}>",
                        "Record<{paperId: 0, citationId: 8}>",
                        "Record<{paperId: 2, citationId: 3}>",
                        "Record<{paperId: 3, citationId: 4}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test8() {
        String query = "MATCH (p:Paper)-[:Resolve]->(ch:Challenge) RETURN count(p) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 168}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test9() {
        String query = "MATCH (a)-[b:WorkOn]-() return count(b) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 123}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test10() {
        String query = "MATCH (a: Paper)-[]-() return count(a) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 336}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test11() {
        String query = "MATCH (a)-[b:Target]-() return count(b) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 69}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test12() {
        String query =
                "MATCH (p:Paper)-[:WorkOn]->(a:Task),(a)-[:Belong]->(t: Topic)\n"
                        + " RETURN t.category,COUNT(p);";
        List expected =
                Arrays.asList(
                        "Record<{category: \"Centrality\", $f1: 18}>",
                        "Record<{category: \"Community Detection\", $f1: 13}>",
                        "Record<{category: \"Similarity\", $f1: 7}>",
                        "Record<{category: \"Cohesive Subgraph\", $f1: 12}>",
                        "Record<{category: \"Pattern Matching\", $f1: 30}>",
                        "Record<{category: \"Traversal\", $f1: 29}>",
                        "Record<{category: \"Covering\", $f1: 14}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test13() {
        String query =
                "MATCH (t: Topic)<-[:Belong]-(a:Task),\n"
                        + " (a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),\n"
                        + " (s)-[:ApplyOn]->(ch:Challenge)\n"
                        + " RETURN t.category, ch.challenge, COUNT(p);";
        List expected =
                Arrays.asList(
                        "Record<{category: \"Centrality\", challenge: \"Parallelism\", $f2: 11}>",
                        "Record<{category: \"Centrality\", challenge: \"Bandwidth\", $f2: 5}>",
                        "Record<{category: \"Centrality\", challenge: \"Communication Overhead\","
                                + " $f2: 9}>",
                        "Record<{category: \"Centrality\", challenge: \"Load Balance\", $f2: 4}>",
                        "Record<{category: \"Community Detection\", challenge: \"Communication"
                                + " Overhead\", $f2: 9}>",
                        "Record<{category: \"Community Detection\", challenge: \"Load Balance\","
                                + " $f2: 4}>",
                        "Record<{category: \"Community Detection\", challenge: \"Parallelism\","
                                + " $f2: 4}>",
                        "Record<{category: \"Similarity\", challenge: \"Communication Overhead\","
                                + " $f2: 7}>",
                        "Record<{category: \"Similarity\", challenge: \"Parallelism\", $f2: 2}>",
                        "Record<{category: \"Similarity\", challenge: \"Load Balance\", $f2: 1}>",
                        "Record<{category: \"Cohesive Subgraph\", challenge: \"Parallelism\", $f2:"
                                + " 7}>",
                        "Record<{category: \"Cohesive Subgraph\", challenge: \"Communication"
                                + " Overhead\", $f2: 9}>",
                        "Record<{category: \"Cohesive Subgraph\", challenge: \"Load Balance\", $f2:"
                                + " 4}>",
                        "Record<{category: \"Pattern Matching\", challenge: \"Load Balance\", $f2:"
                                + " 16}>",
                        "Record<{category: \"Pattern Matching\", challenge: \"Communication"
                                + " Overhead\", $f2: 22}>",
                        "Record<{category: \"Pattern Matching\", challenge: \"Parallelism\", $f2:"
                                + " 1}>",
                        "Record<{category: \"Pattern Matching\", challenge: \"Bandwidth\", $f2:"
                                + " 6}>",
                        "Record<{category: \"Traversal\", challenge: \"Parallelism\", $f2: 12}>",
                        "Record<{category: \"Traversal\", challenge: \"Communication Overhead\","
                                + " $f2: 13}>",
                        "Record<{category: \"Traversal\", challenge: \"Load Balance\", $f2: 8}>",
                        "Record<{category: \"Traversal\", challenge: \"Bandwidth\", $f2: 2}>",
                        "Record<{category: \"Covering\", challenge: \"Parallelism\", $f2: 9}>",
                        "Record<{category: \"Covering\", challenge: \"Communication Overhead\","
                                + " $f2: 4}>",
                        "Record<{category: \"Covering\", challenge: \"Load Balance\", $f2: 1}>");
        return new QueryContext(query, expected);
    }
}
