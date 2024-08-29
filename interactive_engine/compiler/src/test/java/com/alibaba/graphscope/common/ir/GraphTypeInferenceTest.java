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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class GraphTypeInferenceTest {
    // infer types of endpoints given the relation type
    // (?)-[KNOWS]->(?) => (PERSON)-[KNOWS]->(PERSON)
    @Test
    public void schema_type_test_1() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[:KNOWS]->(b) Return a, b",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of vertex a
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
        // check label type of vertex b
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(1).getType()).getLabelType().toString());
    }

    // infer types of endpoints given the relation type, the type is fuzzy
    // (?)-[REPLYOF]->(?) => (COMMENT)-[REPLOYOF]->(POST|COMMENT)
    @Test
    public void schema_type_test_2() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[:REPLYOF]->(b) Return a, b",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of vertex a
        Assert.assertEquals(
                "[VertexLabel(COMMENT)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
        // check label type of vertex b
        Assert.assertEquals(
                "[VertexLabel(POST), VertexLabel(COMMENT)]",
                ((GraphSchemaType) fields.get(1).getType()).getLabelType().toString());
    }

    // infer types of relation given the endpoints type
    // (PERSON)-[?]->(PERSON) => (PERSON)-[KNOWS]->(PERSON)
    @Test
    public void schema_type_test_3() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a:PERSON)-[c]->(b:PERSON) Return c",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of edge c
        Assert.assertEquals(
                "[EdgeLabel(KNOWS, PERSON, PERSON)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
    }

    // infer types of relation given the endpoints type, the direction of the relation is 'BOTH'
    // (PERSON)-[?]-(PERSON) => (PERSON)-[KNOWS]-(PERSON)
    @Test
    public void schema_type_test_4() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (:PERSON)-[b]-(:PERSON) Return b",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of edge b
        Assert.assertEquals(
                "[EdgeLabel(KNOWS, PERSON, PERSON)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
    }

    // infer types of path expand given the endpoints type
    // (PERSON)-[?]->(PERSON) => (PERSON)-[KNOWS]->(PERSON)
    @Test
    public void schema_type_test_5() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a:PERSON)-[c*1..2]->(b:PERSON) Return c",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        GraphPathType pathType = (GraphPathType) match.getRowType().getFieldList().get(0).getType();
        // check label type of getV in path_expand
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) pathType.getComponentType().getGetVType())
                        .getLabelType()
                        .toString());
    }

    // infer types of endpoints given the path expand type
    // (?)-[KNOWS*1..2]->(?) => (PERSON)-[KNOWS*1..2]->(PERSON)
    @Test
    public void schema_type_test_11() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[:KNOWS*1..2]->(b) Return a, b",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of vertex a
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
        // check label type of vertex b
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(1).getType()).getLabelType().toString());
    }

    // (?)-[REPLYOF]->(?)<-[CONTAINEROF]-(?) => (COMMENT)-[REPLYOF]->(POST)<-[CONTAINEROF]-(FORUM)
    @Test
    public void schema_type_test_6() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[b:REPLYOF]->(c)<-[:CONTAINEROF]-(d) Return a, b, c, d",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of vertex a
        Assert.assertEquals(
                "[VertexLabel(COMMENT)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
        // check label type of edge b
        Assert.assertEquals(
                "[EdgeLabel(REPLYOF, COMMENT, POST)]",
                ((GraphSchemaType) fields.get(1).getType()).getLabelType().toString());
        // check label type of vertex c
        Assert.assertEquals(
                "[VertexLabel(POST)]",
                ((GraphSchemaType) fields.get(2).getType()).getLabelType().toString());
        // check label type of vertex d
        Assert.assertEquals(
                "[VertexLabel(FORUM)]",
                ((GraphSchemaType) fields.get(3).getType()).getLabelType().toString());
    }

    // infer types for multiple sentences
    // (?)-[]->(?), (?)-[KNOWS]-(?) => (PERSON)-[KNOWS]->(PERSON)
    @Test
    public void schema_type_test_7() {
        RelNode match =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[c]-(b), (a)-[:KNOWS]-(b) Return a, b, c",
                                Utils.mockGraphBuilder("schema/ldbc.json"))
                        .build();
        List<RelDataTypeField> fields = match.getRowType().getFieldList();
        // check label type of vertex a
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(0).getType()).getLabelType().toString());
        // check label type of vertex b
        Assert.assertEquals(
                "[VertexLabel(PERSON)]",
                ((GraphSchemaType) fields.get(1).getType()).getLabelType().toString());
        // check label type of edge c
        Assert.assertEquals(
                "[EdgeLabel(KNOWS, PERSON, PERSON)]",
                ((GraphSchemaType) fields.get(2).getType()).getLabelType().toString());
    }

    // throw errors if the type of the relation is not compatible with the type of the endpoints
    // (PERSON)-[REPLYOF]->(PERSON)
    @Test
    public void schema_type_test_8() {
        try {
            com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                            "Match (a:PERSON)-[c:REPLYOF]->(b:PERSON) Return c",
                            Utils.mockGraphBuilder("schema/ldbc.json"))
                    .build();
        } catch (Exception e) {
            Assert.assertTrue(
                    e.getMessage()
                            .equals(
                                    "graph schema type error: unable to find getV with [opt=END,"
                                            + " type=[VertexLabel(PERSON)]] from expand with"
                                            + " [type=[EdgeLabel(REPLYOF, COMMENT, COMMENT),"
                                            + " EdgeLabel(REPLYOF, COMMENT, POST)]]"));
            return;
        }
        Assert.fail();
    }

    // check property existence after type inference
    @Test
    public void schema_type_test_9() {
        try {
            com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                            "Match (a)-[:ISLOCATEDIN]->(b {creationDate:20101012}) Return a, b",
                            Utils.mockGraphBuilder("schema/ldbc.json"))
                    .build();
        } catch (FrontendException e) {
            // after type inference, 'b' should be of type 'PLACE', which does not have property
            // 'creationDate'
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "{property=creationDate} not found; expected properties are:"
                                            + " [id, name, url, type]"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void schema_type_test_10() {
        try {
            com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                            "Match (a)-[:ISLOCATEDIN]->(b {creationDate:20101012}) Return a, b",
                            Utils.mockGraphBuilder("schema/ldbc.json"))
                    .build();
        } catch (FrontendException e) {
            // after type inference, 'b' should be of type 'PLACE', which does not have property
            // 'creationDate'
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "{property=creationDate} not found; expected properties are:"
                                            + " [id, name, url, type]"));
            return;
        }
        Assert.fail();
    }
}
