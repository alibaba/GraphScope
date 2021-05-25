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
