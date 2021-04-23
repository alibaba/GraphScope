package com.alibaba.maxgraph.tests.frontend.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.EdgeRelationEntity;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphAlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphAlterVertexTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphCreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphCreateVertexTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphDropEdgeTypeManager;
import com.alibaba.maxgraph.v2.frontend.graph.schema.MaxGraphDropVertexTypeManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaManagerTest {
    @Test
    void testCreateVertexType() {
        CreateVertexTypeManager createVertexTypeManager = new MaxGraphCreateVertexTypeManager("person")
                .addProperty("id", "long")
                .addProperty("name", "string", "name of person")
                .addProperty("age", "int", "age of person, default by 0", 0)
                .comment("person vertex type")
                .primaryKey("id");
        List<String> primaryKeyList = createVertexTypeManager.getPrimaryKeyList();
        assertEquals(Lists.newArrayList("id"), primaryKeyList);
        assertEquals("person", createVertexTypeManager.getLabel());
        assertEquals(3, createVertexTypeManager.getPropertyDefinitions().size());
        assertEquals(Lists.newArrayList("id", "name", "age"),
                createVertexTypeManager.getPropertyDefinitions().stream().map(GraphProperty::getName).collect(Collectors.toList()));
        assertNotNull(createVertexTypeManager.toString());
        System.out.println(createVertexTypeManager.toString());

        // add duplicate property
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateVertexTypeManager("person")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string")
                .primaryKey("id"));
        // primary key dont exist in property list
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateVertexTypeManager("person")
                .addProperty("id", "long")
                .addProperty("name", "string", "name of person")
                .addProperty("age", "int", "age of person, default by 0", 0)
                .comment("person vertex type")
                .primaryKey("id1"));
        // property type invalid
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateVertexTypeManager("person")
                .addProperty("id", "integer")
                .addProperty("name", "string"));
    }

    @Test
    void testAlterVertexType() {
        AlterVertexTypeManager alterVertexTypeManager = new MaxGraphAlterVertexTypeManager("person")
                .dropProperty("gender")
                .addProperty("id", "long")
                .addProperty("name", "string", "name of person")
                .addProperty("age", "int", "age of person, default by 0", 0)
                .comment("alter vertex type");
        assertEquals("person", alterVertexTypeManager.getLabel());
        assertEquals(Lists.newArrayList("gender"), alterVertexTypeManager.getDropPropertyNames());
        assertEquals(3, alterVertexTypeManager.getPropertyDefinitions().size());
        assertEquals(Lists.newArrayList("id", "name", "age"),
                alterVertexTypeManager.getPropertyDefinitions().stream().map(GraphProperty::getName).collect(Collectors.toList()));
        assertNotNull(alterVertexTypeManager.toString());
        System.out.println(alterVertexTypeManager.toString());

        // add duplicate property
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphAlterVertexTypeManager("person")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string"));
        // property type invalid
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphAlterVertexTypeManager("person")
                .addProperty("id", "integer")
                .addProperty("name", "string"));
    }

    @Test
    void testCreateEdgeType() {
        CreateEdgeTypeManager createEdgeTypeManager = new MaxGraphCreateEdgeTypeManager("knows")
                .addProperty("id", "long")
                .addProperty("weight1", "double", "weight1 of knows")
                .addProperty("weight2", "double", "weight2 of knows, default 0", 0)
                .addRelation("person", "person")
                .comment("knows edge between person");
        List<EdgeRelationEntity> relationList = createEdgeTypeManager.getRelationList();
        assertEquals(Lists.newArrayList(new EdgeRelationEntity("person", "person")), relationList);
        assertEquals("knows", createEdgeTypeManager.getLabel());
        assertEquals(3, createEdgeTypeManager.getPropertyDefinitions().size());
        assertEquals(Lists.newArrayList("id", "weight1", "weight2"),
                createEdgeTypeManager.getPropertyDefinitions().stream().map(GraphProperty::getName).collect(Collectors.toList()));
        assertNotNull(createEdgeTypeManager.toString());
        System.out.println(createEdgeTypeManager.toString());

        // add duplicate property
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateEdgeTypeManager("knows")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string"));
        // add duplicate relation
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateEdgeTypeManager("knows")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string")
                .addRelation("person", "person")
                .addRelation("person", "person"));
        // property type invalid
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphCreateVertexTypeManager("person")
                .addProperty("id", "integer")
                .addProperty("name", "string"));
    }

    @Test
    void testAlterEdgeType() {
        AlterEdgeTypeManager alterEdgeTypeManager= new MaxGraphAlterEdgeTypeManager("knows")
                .dropProperty("id1")
                .addProperty("id", "long")
                .addProperty("weight1", "double", "weight1 of knows")
                .addProperty("weight2", "double", "weight2 of knows, default 0", 0)
                .addRelation("person", "person")
                .dropRelation("person1", "person2")
                .comment("knows edge between person");
        List<EdgeRelationEntity> relationList = alterEdgeTypeManager.getAddRelationList();
        assertEquals(Lists.newArrayList(new EdgeRelationEntity("person", "person")), relationList);
        List<EdgeRelationEntity> dropRelationList = alterEdgeTypeManager.getDropRelationList();
        assertEquals(Lists.newArrayList(new EdgeRelationEntity("person1", "person2")), dropRelationList);
        assertEquals("knows", alterEdgeTypeManager.getLabel());
        assertEquals(3, alterEdgeTypeManager.getPropertyDefinitions().size());
        assertEquals(Lists.newArrayList("id", "weight1", "weight2"),
                alterEdgeTypeManager.getPropertyDefinitions().stream().map(GraphProperty::getName).collect(Collectors.toList()));
        assertEquals(Lists.newArrayList("id1"), alterEdgeTypeManager.getDropPropertyNames());
        assertNotNull(alterEdgeTypeManager.toString());
        System.out.println(alterEdgeTypeManager.toString());

        // add duplicate property
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphAlterEdgeTypeManager("knows")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string"));
        // add duplicate relation
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphAlterEdgeTypeManager("knows")
                .addProperty("id", "long")
                .addProperty("id", "string")
                .addProperty("name", "string")
                .addRelation("person", "person")
                .addRelation("person", "person"));
        // property type invalid
        assertThrows(GraphCreateSchemaException.class, () -> new MaxGraphAlterEdgeTypeManager("person")
                .addProperty("id", "integer")
                .addProperty("name", "string"));
    }

    @Test
    void testDropVertexEdgeType() {
        assertEquals("person", new MaxGraphDropVertexTypeManager("person").getLabel());
        assertEquals("knows", new MaxGraphDropEdgeTypeManager("knows").getLabel());
    }
}
