/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tests.frontend.graph;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphWriter;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeRelation;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphProperty;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GraphSchemaTestHelper {
    static void testCreateSchemaType(GraphSchema schema, MaxGraphWriter writer) throws Exception {
        createModernSchema(schema, writer);
        validateModernSchema(schema);
    }

    static void testUpdateSchemaType(GraphSchema schema, MaxGraphWriter writer) throws Exception {
        createModernSchema(schema, writer);
        validateModernSchema(schema);

        writer.addProperty("person", new DefaultGraphProperty("gender", 0, DataType.STRING));
        writer.addProperty("knows", new DefaultGraphProperty("year", 0, DataType.INT));

        SchemaElement personElement = schema.getSchemaElement("person");
        checkSchemaProperties(personElement,
                "person",
                Lists.newArrayList("id", "name", "age", "gender"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.INT, DataType.STRING));
        checkSchemaProperties(schema.getSchemaElement(personElement.getLabelId()),
                "person",
                Lists.newArrayList("id", "name", "age", "gender"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.INT, DataType.STRING));

        SchemaElement knowsElement = schema.getSchemaElement("knows");
        checkSchemaProperties(knowsElement,
                "knows",
                Lists.newArrayList("id", "weight", "year"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE, DataType.INT));
        checkSchemaProperties(schema.getSchemaElement(knowsElement.getLabelId()),
                "knows",
                Lists.newArrayList("id", "weight", "year"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE, DataType.INT));

        writer.dropProperty("person", "gender");
        writer.dropProperty("knows", "year");
        validateModernSchema(schema);

        EdgeType edgeType1 = (EdgeType) schema.getSchemaElement("created");
        assertEquals(1, edgeType1.getRelationList().size());
        List<Pair<String, String>> relationPairList1 = edgeType1.getRelationList()
                .stream()
                .map(v -> Pair.of(v.getSource().getLabel(), v.getTarget().getLabel()))
                .collect(Collectors.toList());
        assertEquals(Lists.newArrayList(Pair.of("person", "software")), relationPairList1);

        writer.addEdgeRelation("created", "software", "person");
        EdgeType edgeType2 = (EdgeType) schema.getSchemaElement("created");
        Set<Pair<String, String>> relationPairSet = edgeType2.getRelationList()
                .stream()
                .map(v -> Pair.of(v.getSource().getLabel(), v.getTarget().getLabel()))
                .collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(Pair.of("person", "software"), Pair.of("software", "person")),
                relationPairSet);

        writer.dropEdgeRelation("created", "software", "person");
        EdgeType edgeType3 = (EdgeType) schema.getSchemaElement("created");
        assertEquals(1, edgeType3.getRelationList().size());
        List<Pair<String, String>> relationPairList2 = edgeType3.getRelationList()
                .stream()
                .map(v -> Pair.of(v.getSource().getLabel(), v.getTarget().getLabel()))
                .collect(Collectors.toList());
        assertEquals(Lists.newArrayList(Pair.of("person", "software")), relationPairList2);

        writer.createEdgeType("testEdgeLabel", Lists.newArrayList(), Lists.newArrayList()).get();
        writer.addProperty("testEdgeLabel", new DefaultGraphProperty("testId1", 0, DataType.LONG));
        writer.addProperty("testEdgeLabel", new DefaultGraphProperty("testId2", 0, DataType.LONG));
        assertEquals(2, schema.getSchemaElement("testEdgeLabel").getPropertyList().size());
        writer.dropProperty("testEdgeLabel", "testId1");
        assertEquals(1, schema.getSchemaElement("testEdgeLabel").getPropertyList().size());
        writer.dropVertexType("testEdgeLabel").get();
        writer.dropEdgeType("testEdgeLabel").get();
        validateModernSchema(schema);
    }

    static void testCheckSchemaProperty(GraphSchema schema, MaxGraphWriter writer) throws Exception {
        createModernSchema(schema, writer);

        Map<Integer, Integer> labelIdProperties = schema.getPropertyId("id");
        assertTrue(labelIdProperties.size() > 0);
        labelIdProperties.forEach((k, v) -> {
            assertEquals("id", schema.getPropertyName(v).values().iterator().next());
        });

        Map<Integer, Integer> labelNameProperties = schema.getPropertyId("name");
        assertTrue(labelNameProperties.size() > 0);
        labelNameProperties.forEach((k, v) -> {
            assertEquals("name", schema.getPropertyName(v).values().iterator().next());
        });

        Map<Integer, Integer> labelWeightProperty = schema.getPropertyId("weight");
        assertTrue(labelWeightProperty.size() > 0);
        labelWeightProperty.forEach((k, v) -> {
            assertEquals("weight", schema.getPropertyName(v).values().iterator().next());
        });

        Map<Integer, GraphProperty> schemaIdProperties = schema.getPropertyDefinitions("id");
        assertEquals(4, schemaIdProperties.size());
    }

    static void testInvalidSchemaType(DefaultGraphSchema schema, DefaultMaxGraphWriter writer) throws Exception {
        createModernSchema(schema, writer);
        assertThrows(GraphCreateSchemaException.class,
                () -> writer.createVertexType("person",
                        Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                                new DefaultGraphProperty("name", 0, DataType.STRING),
                                new DefaultGraphProperty("age", 0, DataType.INT)),
                        Lists.newArrayList("id")).get());
        assertThrows(GraphCreateSchemaException.class,
                () -> writer.createVertexType("persontmp1",
                        Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                                new DefaultGraphProperty("name", 0, DataType.STRING),
                                new DefaultGraphProperty("age", 0, DataType.INT)),
                        Lists.newArrayList()).get());
        assertThrows(GraphCreateSchemaException.class,
                () -> writer.createEdgeType("knows",
                        Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                                new DefaultGraphProperty("name", 0, DataType.STRING),
                                new DefaultGraphProperty("age", 0, DataType.INT)),
                        Lists.newArrayList()).get());
        assertThrows(GraphCreateSchemaException.class,
                () -> writer.addEdgeRelation("knows1", "person", "person").get());
        assertThrows(GraphCreateSchemaException.class,
                () -> writer.dropEdgeRelation("knows1", "person", "person").get());
    }

    static void createModernSchema(GraphSchema schema, MaxGraphWriter writer) throws Exception {
        writer.createVertexType("person",
                Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                        new DefaultGraphProperty("name", 0, DataType.STRING),
                        new DefaultGraphProperty("age", 0, DataType.INT)),
                Lists.newArrayList("id")).get();
        writer.createVertexType("software",
                Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                        new DefaultGraphProperty("name", 0, DataType.STRING),
                        new DefaultGraphProperty("lang", 0, DataType.STRING)),
                Lists.newArrayList("id")).get();

        VertexType personVertexType = (VertexType) schema.getSchemaElement("person");
        VertexType softwareVertexType = (VertexType) schema.getSchemaElement("software");
        writer.createEdgeType("knows",
                Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                        new DefaultGraphProperty("weight", 0, DataType.DOUBLE)),
                Lists.newArrayList(new DefaultEdgeRelation(personVertexType, personVertexType))).get();
        writer.createEdgeType("created",
                Lists.newArrayList(new DefaultGraphProperty("id", 0, DataType.LONG),
                        new DefaultGraphProperty("weight", 0, DataType.DOUBLE)),
                Lists.newArrayList(new DefaultEdgeRelation(personVertexType, softwareVertexType))).get();
    }

    private static void validateModernSchema(GraphSchema schema) {
        assertTrue(schema.getVersion() >= 0);
        assertEquals(2, schema.getVertexTypes().size());
        assertEquals(2, schema.getEdgeTypes().size());

        SchemaElement personElement = schema.getSchemaElement("person");
        checkSchemaProperties(personElement,
                "person",
                Lists.newArrayList("id", "name", "age"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.INT));
        checkSchemaProperties(schema.getSchemaElement(personElement.getLabelId()),
                "person",
                Lists.newArrayList("id", "name", "age"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.INT));
        assertEquals("id", schema.getPropertyDefinition(personElement.getLabelId(), personElement.getProperty("id").getId()).getName());

        SchemaElement softwareElement = schema.getSchemaElement("software");
        checkSchemaProperties(softwareElement,
                "software",
                Lists.newArrayList("id", "name", "lang"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.STRING));
        checkSchemaProperties(schema.getSchemaElement(softwareElement.getLabelId()),
                "software",
                Lists.newArrayList("id", "name", "lang"),
                Lists.newArrayList(DataType.LONG, DataType.STRING, DataType.STRING));

        SchemaElement knowsElement = schema.getSchemaElement("knows");
        checkSchemaProperties(knowsElement,
                "knows",
                Lists.newArrayList("id", "weight"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE));
        checkSchemaProperties(schema.getSchemaElement(knowsElement.getLabelId()),
                "knows",
                Lists.newArrayList("id", "weight"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE));

        SchemaElement createdElement = schema.getSchemaElement("created");
        checkSchemaProperties(createdElement,
                "created",
                Lists.newArrayList("id", "weight"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE));
        checkSchemaProperties(schema.getSchemaElement(createdElement.getLabelId()),
                "created",
                Lists.newArrayList("id", "weight"),
                Lists.newArrayList(DataType.LONG, DataType.DOUBLE));

        assertNotNull(schema.toString());
    }

    private static void checkSchemaProperties(SchemaElement element, String expectedLabel, List<String> propertyNames, List<DataType> propertyDataTypes) {
        assertEquals(propertyNames.size(), propertyDataTypes.size());
        assertEquals(expectedLabel, element.getLabel());
        assertTrue(element.getLabelId() > 0);

        Set<Integer> propertyIds = Sets.newHashSet();
        assertEquals(propertyNames.size(), element.getPropertyList().size());
        for (int i = 0; i < propertyNames.size(); i++) {
            String propertyName = propertyNames.get(i);
            DataType propertyDataType = propertyDataTypes.get(i);
            GraphProperty property = element.getProperty(propertyName);
            checkProperty(propertyIds, property, propertyName, propertyDataType);
            assertEquals(property, element.getProperty(property.getId()));
        }
    }

    private static void checkProperty(Set<Integer> propertyIds,
                                      GraphProperty property,
                                      String expectedName,
                                      DataType expectedDataType) {
        assertTrue(property.getId() > 0);
        assertTrue(!propertyIds.contains(property.getId()));
        assertEquals(expectedName, property.getName());
        assertEquals(expectedDataType, property.getDataType());
        propertyIds.add(property.getId());
    }
}
