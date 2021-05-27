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
