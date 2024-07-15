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
                "MATCH (p:Paper)-[:WorkOn]->(:Task)-[:Belong]->(topic:Topic)\n"
                        + "WITH distinct topic, count(p) as paperCount\n"
                        + "RETURN topic.topic, paperCount\n "
                        + "ORDER BY paperCount desc;";
        List<String> expected =
                Arrays.asList(
                        "Record<{topic: \"Pattern Matching\", paperCount: 30}>",
                        "Record<{topic: \"Traversal\", paperCount:" + " 29}>",
                        "Record<{topic: \"Centrality\", paperCount: 18}>",
                        "Record<{topic: \"Covering\", paperCount: 14}>",
                        "Record<{topic: \"Community Detection\", paperCount: 13}>",
                        "Record<{topic: \"Cohesive Subgraph\", paperCount: 12}>",
                        "Record<{topic: \"Similarity\", paperCount: 7}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test2() {
        String query =
                "MATCH (t:Topic)<-[:Belong]-(a:Task),\n"
                        + "(a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),\n"
                        + "(s)-[:ApplyOn]->(ch:Challenge)\n"
                        + "WITH t, ch, count(p) AS num \n"
                        + "RETURN t.topic AS topic, ch.challenge AS challenge, num\n"
                        + "ORDER BY num DESC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{topic: \"Pattern Matching\", challenge:"
                                + " \"Communication Overhead\", num: 22}>",
                        "Record<{topic: \"Pattern Matching\", challenge:"
                                + " \"Load Balance\", num: 16}>",
                        "Record<{topic: \"Traversal\", challenge:"
                                + " \"Communication Overhead\", num: 13}>",
                        "Record<{topic: \"Traversal\", challenge: \"Parallelism" + "\", num: 12}>",
                        "Record<{topic: \"Centrality\", challenge: \"Parallelism\", num:"
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

    public static QueryContext get_graph_algo_test9_0() {
        String query = "MATCH (a)-[b:WorkOn]->() return count(b) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 123}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test9_1() {
        String query = "MATCH (a)-[b:WorkOn]-() return count(b) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 246}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test10() {
        String query = "MATCH (a: Paper)-[]->() return count(a) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 685}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test11() {
        String query = "MATCH (a)-[b:Target]-() return count(b) AS count;";
        List<String> expected = Arrays.asList("Record<{count: 114}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test12() {
        String query =
                "MATCH (p:Paper)-[:WorkOn]->(a:Task),(a)-[:Belong]->(t: Topic)\n"
                        + " RETURN t.topic,COUNT(p);";
        List expected =
                Arrays.asList(
                        "Record<{topic: \"Centrality\", $f1: 18}>",
                        "Record<{topic: \"Community Detection\", $f1: 13}>",
                        "Record<{topic: \"Similarity\", $f1: 7}>",
                        "Record<{topic: \"Cohesive Subgraph\", $f1: 12}>",
                        "Record<{topic: \"Pattern Matching\", $f1: 30}>",
                        "Record<{topic: \"Traversal\", $f1: 29}>",
                        "Record<{topic: \"Covering\", $f1: 14}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test13() {
        String query =
                "MATCH (t: Topic)<-[:Belong]-(a:Task),\n"
                        + " (a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Solution),\n"
                        + " (s)-[:ApplyOn]->(ch:Challenge)\n"
                        + " RETURN t.topic, ch.challenge, COUNT(p);";
        List expected =
                Arrays.asList(
                        "Record<{topic: \"Centrality\", challenge: \"Parallelism\", $f2: 11}>",
                        "Record<{topic: \"Centrality\", challenge: \"Bandwidth\", $f2: 5}>",
                        "Record<{topic: \"Centrality\", challenge: \"Communication Overhead\","
                                + " $f2: 9}>",
                        "Record<{topic: \"Centrality\", challenge: \"Load Balance\", $f2: 4}>",
                        "Record<{topic: \"Community Detection\", challenge: \"Communication"
                                + " Overhead\", $f2: 9}>",
                        "Record<{topic: \"Community Detection\", challenge: \"Load Balance\","
                                + " $f2: 4}>",
                        "Record<{topic: \"Community Detection\", challenge: \"Parallelism\","
                                + " $f2: 4}>",
                        "Record<{topic: \"Similarity\", challenge: \"Communication Overhead\","
                                + " $f2: 7}>",
                        "Record<{topic: \"Similarity\", challenge: \"Parallelism\", $f2: 2}>",
                        "Record<{topic: \"Similarity\", challenge: \"Load Balance\", $f2: 1}>",
                        "Record<{topic: \"Cohesive Subgraph\", challenge: \"Parallelism\", $f2:"
                                + " 7}>",
                        "Record<{topic: \"Cohesive Subgraph\", challenge: \"Communication"
                                + " Overhead\", $f2: 9}>",
                        "Record<{topic: \"Cohesive Subgraph\", challenge: \"Load Balance\", $f2:"
                                + " 4}>",
                        "Record<{topic: \"Pattern Matching\", challenge: \"Load Balance\", $f2:"
                                + " 16}>",
                        "Record<{topic: \"Pattern Matching\", challenge: \"Communication"
                                + " Overhead\", $f2: 22}>",
                        "Record<{topic: \"Pattern Matching\", challenge: \"Parallelism\", $f2:"
                                + " 1}>",
                        "Record<{topic: \"Pattern Matching\", challenge: \"Bandwidth\", $f2:"
                                + " 6}>",
                        "Record<{topic: \"Traversal\", challenge: \"Parallelism\", $f2: 12}>",
                        "Record<{topic: \"Traversal\", challenge: \"Communication Overhead\","
                                + " $f2: 13}>",
                        "Record<{topic: \"Traversal\", challenge: \"Load Balance\", $f2: 8}>",
                        "Record<{topic: \"Traversal\", challenge: \"Bandwidth\", $f2: 2}>",
                        "Record<{topic: \"Covering\", challenge: \"Parallelism\", $f2: 9}>",
                        "Record<{topic: \"Covering\", challenge: \"Communication Overhead\","
                                + " $f2: 4}>",
                        "Record<{topic: \"Covering\", challenge: \"Load Balance\", $f2: 1}>");
        return new QueryContext(query, expected);
    }
}
