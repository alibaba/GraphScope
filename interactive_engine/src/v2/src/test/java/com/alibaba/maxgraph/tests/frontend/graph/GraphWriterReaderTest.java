package com.alibaba.maxgraph.tests.frontend.graph;

import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphReader;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphWriter;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMemoryGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultSchemaFetcher;
import org.junit.jupiter.api.Test;

import static com.alibaba.maxgraph.tests.frontend.graph.GraphWriterReaderTestHelper.*;

public class GraphWriterReaderTest extends AbstractGraphTest {

    @Test
    void testCreateDefaultGraphData() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultSchemaFetcher schemaFetcher = new DefaultSchemaFetcher(schema);
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        DefaultMaxGraphReader reader = new DefaultMaxGraphReader(writer, graph, schemaFetcher);
        SnapshotMaxGraph maxGraph = new SnapshotMaxGraph();
        maxGraph.initialize(reader, writer, schemaFetcher);
        testCreateGraphData(maxGraph, writer, schema);
    }

    @Test
    void testDefaultVertexEdgeProperty() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultSchemaFetcher schemaFetcher = new DefaultSchemaFetcher(schema);
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        DefaultMaxGraphReader reader = new DefaultMaxGraphReader(writer, graph, schemaFetcher);
        SnapshotMaxGraph maxGraph = new SnapshotMaxGraph();
        maxGraph.initialize(reader, writer, schemaFetcher);
        testVertexEdgeProperty(maxGraph, writer, schema);
    }

    @Test
    void testDefaultDirectionElementProperty() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultSchemaFetcher schemaFetcher = new DefaultSchemaFetcher(schema);
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        DefaultMaxGraphReader reader = new DefaultMaxGraphReader(writer, graph, schemaFetcher);
        SnapshotMaxGraph maxGraph = new SnapshotMaxGraph();
        maxGraph.initialize(reader, writer, schemaFetcher);
        testDirectionElementProperty(maxGraph, writer, schema);
    }

    @Test
    void testDefaultMaxTinkerGraph() throws Exception {
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultSchemaFetcher schemaFetcher = new DefaultSchemaFetcher(schema);
        DefaultMemoryGraph graph = new DefaultMemoryGraph();
        DefaultMaxGraphWriter writer = new DefaultMaxGraphWriter(schema, graph);
        DefaultMaxGraphReader reader = new DefaultMaxGraphReader(writer, graph, schemaFetcher);
        SnapshotMaxGraph maxGraph = new SnapshotMaxGraph();
        maxGraph.initialize(reader, writer, schemaFetcher);
        testMaxTinkerGraph(maxGraph);
    }
}
