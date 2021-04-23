package com.alibaba.maxgraph.tests.frontend.graph;

import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphWriter;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMemoryGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import org.junit.jupiter.api.Test;

public class GraphSchemaTest extends AbstractGraphTest {
    @Test
    void testCreateDefaultSchemaType() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        GraphSchemaTestHelper.testCreateSchemaType(schema, writer);
    }

    @Test
    void testUpdateDefaultSchemaType() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        GraphSchemaTestHelper.testUpdateSchemaType(schema, writer);
    }

    @Test
    void testCheckDefaultSchemaProperty() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        GraphSchemaTestHelper.testCheckSchemaProperty(schema, writer);
    }

    @Test
    void testInvalidDefaultSchemaType() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);

        GraphSchemaTestHelper.testInvalidSchemaType(schema, writer);
    }

    ///TODO

    @Test
    void testCreateRemoteSchemaType() throws Exception {
    }

    @Test
    void testUpdateRemoteSchemaType() throws Exception {
    }

    @Test
    void testCheckRemoteSchemaProperty() throws Exception {
    }
    @Test
    void testInvalidRemoteSchemaType() throws Exception {
    }

}
