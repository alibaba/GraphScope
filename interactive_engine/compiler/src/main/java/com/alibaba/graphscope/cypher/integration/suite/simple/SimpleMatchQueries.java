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
package com.alibaba.graphscope.cypher.integration.suite.simple;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;

import java.util.Arrays;
import java.util.List;

public class SimpleMatchQueries {

    public static QueryContext get_simple_match_query_1_test() {
        String query = "MATCH(p) with p ORDER BY p.id ASC  LIMIT 10 RETURN p.id;";
        List<String> expected =
                Arrays.asList(
                        "Record<{id: 0}>",
                        "Record<{id: 0}>",
                        "Record<{id: 0}>",
                        "Record<{id: 0}>",
                        "Record<{id: 0}>",
                        "Record<{id: 1}>",
                        "Record<{id: 1}>",
                        "Record<{id: 1}>",
                        "Record<{id: 2}>",
                        "Record<{id: 2}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_2_test() {
        String query = "MATCH(p) RETURN COUNT(p) AS VerticesNum;";
        List<String> expected = Arrays.asList("Record<{VerticesNum: 327588}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_3_test() {
        String query = "MATCH(p : PERSON) RETURN COUNT(p) AS numPerson;";
        List<String> expected = Arrays.asList("Record<{numPerson: 1528}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_4_test() {
        String query = "MATCH(a:PLACE)-[b]->(c) return COUNT(b) AS numPlacesEdges;";
        List<String> expected = Arrays.asList("Record<{numPlacesEdges: 1454}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_5_test() {
        String query =
                "MATCH(a:PERSON)-[:KNOWS]-(p:PERSON {id : 6597069767117}) return COUNT(p) AS"
                        + " numFriends;";
        List<String> expected = Arrays.asList("Record<{numFriends: 9}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_6_test() {
        String query =
                "MATCH(a: PERSON) where a.id = 933L return a.firstName AS firstName, a.lastName as"
                        + " lastName;";
        List<String> expected =
                Arrays.asList("Record<{firstName: \"Mahinda\", lastName: \"Perera\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_7_test() {
        String query =
                "MATCH(a:PERSON)-[b: STUDYAT]->(c) RETURN b.classYear AS classYear ORDER BY"
                        + " classYear DESC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{classYear: 2012}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>",
                        "Record<{classYear: 2011}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_8_test() {
        String query =
                "MATCH(a:PERSON)-[b: STUDYAT]->(c) where b.classYear < 2009 return b.classYear AS"
                        + " classYear ORDER BY classYear DESC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>",
                        "Record<{classYear: 2008}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_9_test() {
        String query =
                "MATCH(a)-[b: CONTAINEROF]->(c) return c.id AS postId ORDER BY postId ASC LIMIT"
                        + " 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{postId: 3}>",
                        "Record<{postId: 32484}>",
                        "Record<{postId: 32693}>",
                        "Record<{postId: 32756}>",
                        "Record<{postId: 32782}>",
                        "Record<{postId: 32833}>",
                        "Record<{postId: 32843}>",
                        "Record<{postId: 32903}>",
                        "Record<{postId: 33041}>",
                        "Record<{postId: 33042}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_10_test() {
        String query =
                "MATCH( a {id:933})-[b]-(c {id: 2199023256077}) return labels(a) AS"
                        + " vertexLabelName, type(b) AS edgeLabelName;";
        List<String> expected =
                Arrays.asList("Record<{vertexLabelName: \"PERSON\", edgeLabelName: \"KNOWS\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_11_test() {
        String query = "Match( p: PLACE) return p ORDER BY p.id LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{p: node<0>}>",
                        "Record<{p: node<1>}>",
                        "Record<{p: node<2>}>",
                        "Record<{p: node<3>}>",
                        "Record<{p: node<4>}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_12_test() {
        String query =
                "MATCH(p)<-[:ISLOCATEDIN*1..2]-(a : POST | COMMENT) WITH DISTINCT p, a RETURN p.id"
                    + " AS placeId, p.name AS placeName, a.id AS postOrCommentId ORDER BY placeId"
                    + " ASC, postOrCommentId ASC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{placeId: 0, placeName: \"India\", postOrCommentId: 54780}>",
                        "Record<{placeId: 0, placeName: \"India\", postOrCommentId: 54971}>",
                        "Record<{placeId: 0, placeName: \"India\", postOrCommentId: 54972}>",
                        "Record<{placeId: 0, placeName: \"India\", postOrCommentId: 54973}>",
                        "Record<{placeId: 0, placeName: \"India\", postOrCommentId: 54974}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_13_test() {
        String query =
                "MATCH(a)-[*1..2]->(b) WITH a.id AS aId, b.id AS bId RETURN aId, bId ORDER BY aId"
                        + " ASC, bId ASC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{aId: 0, bId: 3}>",
                        "Record<{aId: 0, bId: 59}>",
                        "Record<{aId: 0, bId: 349}>",
                        "Record<{aId: 0, bId: 933}>",
                        "Record<{aId: 0, bId: 1454}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_14_test() {
        String query =
                "Match (a)-[c*0..2]->(b) RETURN a.id AS aId, c, b.id AS bId ORDER BY aId"
                        + " ASC, bId ASC LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{aId: 0, c: path[], bId: 0}>",
                        "Record<{aId: 0, c: path[], bId: 0}>",
                        "Record<{aId: 0, c: path[], bId: 0}>",
                        "Record<{aId: 0, c: path[], bId: 0}>",
                        "Record<{aId: 0, c: path[], bId: 3}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_15_test() {
        String query =
                "Match (a:PERSON)-[c:KNOWS*1..2]->(b:PERSON) RETURN a.id AS aId, c, b.id AS bId"
                        + " ORDER BY aId ASC, bId ASC LIMIT 5;";

        List<String> expected =
                Arrays.asList(
                        "Record<{aId: 94, c: path[], bId: 987}>",
                        "Record<{aId: 94, c: path[], bId: 2199023256154}>",
                        "Record<{aId: 94, c: path[], bId: 2199023256776}>",
                        "Record<{aId: 94, c: path[], bId: 6597069767432}>",
                        "Record<{aId: 94, c: path[], bId: 8796093022369}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_simple_match_query_16_test() {
        String query = "Match (a)-[c*0..2]->(b) RETURN COUNT (DISTINCT c);";
        List<String> expected = Arrays.asList("Record<{$f0: 1552803}>");
        return new QueryContext(query, expected);
    }
}
