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
                "MATCH (p:Paper)-[:WorkOn]->(:Algorithm)-[:Belong]->(ca:Topic)\n"
                        + "WITH distinct ca, count(p) as paperCount\n"
                        + "RETURN ca.category, paperCount\n "
                        + "ORDER BY paperCount desc;";
        List<String> expected =
                Arrays.asList(
                        "Record<{category: \"Traversal\", paperCount: 29}>",
                        "Record<{category: \"Cohesive_Subgraph\", paperCount:" + " 18}>",
                        "Record<{category: \"Centrality\", paperCount: 18}>",
                        "Record<{category: \"Clustering\", paperCount: 17}>",
                        "Record<{category: \"Pattern Matching\", paperCount: 11}>",
                        "Record<{category: \"Other\", paperCount: 7}>",
                        "Record<{category: \"Similarity\", paperCount: 2}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_graph_algo_test2() {
        String query =
                "MATCH (ca:Topic)<-[:Belong]-(a:Algorithm),\n"
                        + "(a)<-[:WorkOn]-(p:Paper)-[:Use]->(s:Strategy),\n"
                        + "(s)-[:ApplyOn]->(ch:Challenge)\n"
                        + "WITH ca, ch, count(p) AS num \n"
                        + "RETURN ca.category AS category, ch.challenge AS challenge, num\n"
                        + "ORDER BY num DESC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{category: \"Traversal\", challenge: \"Communication Overhead\","
                                + " num: 8}>",
                        "Record<{category: \"Traversal\", challenge: \"Load Balance\", num: 8}>",
                        "Record<{category: \"Cohesive_Subgraph\", challenge:"
                                + " \"Communication Overhead\", num: 8}>",
                        "Record<{category: \"Cohesive_Subgraph\", challenge:"
                                + " \"Parallelism\", num: 5}>",
                        "Record<{category: \"Pattern Matching\", challenge: \"Load Balance\", num:"
                                + " 3}>");
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
                "MATCH(c: CCFField)<-[e]-(p:Paper) where type(e) = 6 return type(e) As edgeType"
                        + " ORDER BY edgeType ASC LIMIT 10;";
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
}
