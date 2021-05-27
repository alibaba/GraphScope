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
package com.alibaba.maxgraph.tests.frontend.server;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.frontend.api.MaxGraphServer;
import com.alibaba.maxgraph.v2.common.frontend.api.io.MaxGraphIORegistry;
import com.alibaba.maxgraph.v2.common.frontend.driver.MaxGraphResolverSupplier;
import com.alibaba.maxgraph.v2.common.frontend.driver.ser.MaxGraphMessageSerializerV3d0;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.config.GraphStoreType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultSchemaFetcher;
import com.alibaba.maxgraph.v2.frontend.server.MaxGraphServerImpl;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaxGraphServerTest {
    @Test
    void testMaxGraphServer() throws Exception {
        new File("/tmp/generate-classic.groovy").deleteOnExit();
        Configs frontendConfig = Configs.newBuilder()
                .put(FrontendConfig.GRAPH_STORE_TYPE.getKey(), GraphStoreType.MEMORY.name())
                .build();
        DefaultGraphSchema schema = new DefaultGraphSchema();
        DefaultSchemaFetcher schemaFetcher = new DefaultSchemaFetcher(schema);
        MaxGraphServer server = new MaxGraphServerImpl(frontendConfig,
                schemaFetcher,
                null,
                null,
                null,
                null,
                1,
                null);
        server.start();
        int gremlinServerPort = server.getGremlinServerPort();

        GryoMapper.Builder mapper = GryoMapper.build().addRegistry(MaxGraphIORegistry.instance())
                .classResolver(new MaxGraphResolverSupplier());
        MessageSerializer serializer = new MaxGraphMessageSerializerV3d0(mapper);

        Cluster cluster = Cluster.build().addContactPoint("localhost").port(gremlinServerPort).serializer(serializer).create();
        Client client = cluster.connect();

        testCreatePersonKnowsSchema(client);
        testBuildPersonKnowsGraph(client);
        testPersonKnowsGraphQuery(client);

        client.close();
        cluster.close();
        server.stop();
    }

    private void testPersonKnowsGraphQuery(Client client) {
        Iterator<Result> vertexIterator = client.submit("g.V()").iterator();
        Set<Long> vertexIds = Sets.newHashSet();
        while (vertexIterator.hasNext()) {
            vertexIds.add((Long) vertexIterator.next().getVertex().property("id").value());
        }
        assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L, 5L, 6L),
                vertexIds);
    }

    private void testCreatePersonKnowsSchema(Client client) {
        Iterator<Result> createPersonVertexTypeIterator = client.submit("graph.createVertexType('person')" +
                ".addProperty('id', 'long')" +
                ".addProperty('name', 'string')" +
                ".addProperty('age', 'int')" +
                ".primaryKey('id')")
                .iterator();
        assertTrue(createPersonVertexTypeIterator.hasNext());
        assertEquals("create vertex type person success", createPersonVertexTypeIterator.next().getString());
        assertFalse(createPersonVertexTypeIterator.hasNext());

        Iterator<Result> createKnowsEdgeTypeIterator = client.submit("graph.createEdgeType('knows')" +
                ".addProperty('id', 'long')" +
                ".addProperty('weight', 'double')" +
                ".addRelation('person', 'person')")
                .iterator();
        assertTrue(createKnowsEdgeTypeIterator.hasNext());
        assertEquals("create edge type knows success", createKnowsEdgeTypeIterator.next().getString());
        assertFalse(createKnowsEdgeTypeIterator.hasNext());

        Iterator<Result> createTempVertexTypeIterator = client.submit("graph.createVertexType('temp')" +
                ".addProperty('id', 'long')" +
                ".addProperty('name', 'string')" +
                ".addProperty('age', 'int')" +
                ".primaryKey('id')")
                .iterator();
        assertTrue(createTempVertexTypeIterator.hasNext());
        assertEquals("create vertex type temp success", createTempVertexTypeIterator.next().getString());
        assertFalse(createTempVertexTypeIterator.hasNext());

        Iterator<Result> createTempSelfEdgeTypeIterator = client.submit("graph.createEdgeType('self').addRelation('temp', 'temp')").iterator();
        assertTrue(createTempSelfEdgeTypeIterator.hasNext());
        assertEquals("create edge type self success", createTempSelfEdgeTypeIterator.next().getString());
        assertFalse(createTempSelfEdgeTypeIterator.hasNext());

        Iterator<Result> alterTempVertexTypeIterator = client.submit("graph.alterVertexType('temp')" +
                ".addProperty('test_prop', 'int')" +
                ".dropProperty('name')")
                .iterator();
        assertTrue(alterTempVertexTypeIterator.hasNext());
        System.out.println(alterTempVertexTypeIterator.next().getString());
        assertFalse(alterTempVertexTypeIterator.hasNext());

        Iterator<Result> alterTempSelfEdgeTypeIterator = client.submit("graph.alterEdgeType('self')" +
                ".addProperty('id', 'long')").iterator();
        assertTrue(alterTempSelfEdgeTypeIterator.hasNext());
        System.out.println(alterTempSelfEdgeTypeIterator.next().getString());
        assertFalse(alterTempSelfEdgeTypeIterator.hasNext());

        Iterator<Result> dropTempVertexTypeIterator = client.submit("graph.dropVertexType('temp')")
                .iterator();
        assertTrue(dropTempVertexTypeIterator.hasNext());
        System.out.println(dropTempVertexTypeIterator.next().getString());
        assertFalse(dropTempVertexTypeIterator.hasNext());

        Iterator<Result> dropSelfEdgeTypeIterator = client.submit("graph.dropEdgeType('self')")
                .iterator();
        assertTrue(dropSelfEdgeTypeIterator.hasNext());
        System.out.println(dropSelfEdgeTypeIterator.next().getString());
        assertFalse(dropSelfEdgeTypeIterator.hasNext());
    }

    private void testBuildPersonKnowsGraph(Client client) {
        Vertex v1 = client.submit("graph.addVertex(label, 'person', 'id', 1L, 'name', 'tom', 'age', 20)").one().getVertex();
        Vertex v2 = client.submit("graph.addVertex(label, 'person', 'id', 2L, 'name', 'jack', 'age', 25)").one().getVertex();
        Vertex v3 = client.submit("graph.addVertex(label, 'person', 'id', 3L, 'name', 'tim', 'age', 30)").one().getVertex();
        Vertex v4 = client.submit("graph.addVertex(label, 'person', 'id', 4L, 'name', 'tony', 'age', 35)").one().getVertex();
        Vertex v5 = client.submit("graph.addVertex(label, 'person', 'id', 5L, 'name', 'jim', 'age', 40)").one().getVertex();
        Vertex v6 = client.submit("graph.addVertex(label, 'person', 'id', 6L, 'name', 'lili', 'age', 45)").one().getVertex();

        Iterator<Result> addV1KnowsV2Iterator = client.submit("g.V('" + v1.id() + "').addE('knows').property('weight', 0.5d).to(g.V('" + v2.id() + "'))").iterator();
        assertTrue(addV1KnowsV2Iterator.hasNext());
        Edge edge1 = addV1KnowsV2Iterator.next().getEdge();
        assertEquals("knows", edge1.label());
        assertEquals(0.5, edge1.property("weight").value());
        assertEquals(v1.id(), edge1.outVertex().id());
        assertEquals(v2.id(), edge1.inVertex().id());
        assertFalse(addV1KnowsV2Iterator.hasNext());

        Iterator<Result> addV2KnowsV3Iterator = client.submit("g.V('" + v2.id() + "').addE('knows').property('weight', 0.6d).to(g.V('" + v3.id() + "'))").iterator();
        assertTrue(addV2KnowsV3Iterator.hasNext());
        Edge edge2 = addV2KnowsV3Iterator.next().getEdge();
        assertEquals("knows", edge2.label());
        assertEquals(0.6, edge2.property("weight").value());
        assertEquals(v2.id(), edge2.outVertex().id());
        assertEquals(v3.id(), edge2.inVertex().id());
        assertFalse(addV2KnowsV3Iterator.hasNext());

        Iterator<Result> addV3KnowsV4Iterator = client.submit("g.V('" + v3.id() + "').addE('knows').property('weight', 0.7d).to(g.V('" + v4.id() + "'))").iterator();
        assertTrue(addV3KnowsV4Iterator.hasNext());
        Edge edge3 = addV3KnowsV4Iterator.next().getEdge();
        assertEquals("knows", edge3.label());
        assertEquals(0.7, edge3.property("weight").value());
        assertEquals(v3.id(), edge3.outVertex().id());
        assertEquals(v4.id(), edge3.inVertex().id());
        assertFalse(addV3KnowsV4Iterator.hasNext());

        Iterator<Result> addV4KnowsV5Iterator = client.submit("g.V('" + v4.id() + "').addE('knows').property('weight', 0.8d).to(g.V('" + v5.id() + "'))").iterator();
        assertTrue(addV4KnowsV5Iterator.hasNext());
        Edge edge4 = addV4KnowsV5Iterator.next().getEdge();
        assertEquals("knows", edge4.label());
        assertEquals(0.8, edge4.property("weight").value());
        assertEquals(v4.id(), edge4.outVertex().id());
        assertEquals(v5.id(), edge4.inVertex().id());
        assertFalse(addV4KnowsV5Iterator.hasNext());

        Iterator<Result> addV5KnowsV6Iterator = client.submit("g.V('" + v5.id() + "').addE('knows').property('weight', 0.9d).to(g.V('" + v6.id() + "'))").iterator();
        assertTrue(addV5KnowsV6Iterator.hasNext());
        Edge edge5 = addV5KnowsV6Iterator.next().getEdge();
        assertEquals("knows", edge5.label());
        assertEquals(0.9, edge5.property("weight").value());
        assertEquals(v5.id(), edge5.outVertex().id());
        assertEquals(v6.id(), edge5.inVertex().id());
        assertFalse(addV5KnowsV6Iterator.hasNext());

        Iterator<Result> addV6KnowsV1Iterator = client.submit("g.V('" + v6.id() + "').addE('knows').property('weight', 1.0d).to(g.V('" + v1.id() + "'))").iterator();
        assertTrue(addV6KnowsV1Iterator.hasNext());
        Edge edge6 = addV6KnowsV1Iterator.next().getEdge();
        assertEquals("knows", edge6.label());
        assertEquals(1.0, edge6.property("weight").value());
        assertEquals(v6.id(), edge6.outVertex().id());
        assertEquals(v1.id(), edge6.inVertex().id());
        assertFalse(addV6KnowsV1Iterator.hasNext());
    }
}
