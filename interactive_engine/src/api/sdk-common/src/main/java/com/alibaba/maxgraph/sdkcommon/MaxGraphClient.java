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
package com.alibaba.maxgraph.sdkcommon;

import com.alibaba.maxgraph.sdkcommon.manager.ExecuteAlterEdgeRelationManager;
import com.alibaba.maxgraph.sdkcommon.manager.ExecuteAlterVertexEdgeSchemaManager;
import com.alibaba.maxgraph.sdkcommon.manager.ExecuteEdgeSchemaManager;
import com.alibaba.maxgraph.sdkcommon.manager.ExecuteVertexSchemaManager;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface MaxGraphClient extends RemoteConnection {
    /**
     * Execute query script and return result iterator
     *
     * @param script The given query script
     * @return The result iterator
     * @throws Exception The thrown exception
     */
    Iterator<Result> executeQuery(String script) throws Exception;

    /**
     * Execute query script and return result iterator
     *
     * @param script  The given query script
     * @param timeout The given mill timeout
     * @return The result iterator
     * @throws Exception The thrown exception
     */
    Iterator<Result> executeQuery(String script, long timeout) throws Exception;

    /**
     * Execute query script and return result iterator
     *
     * @param script   The given query script
     * @param timeout  The given timeout
     * @param timeUnit The given time unit
     * @return The result iterator
     * @throws Exception The thrown exception
     */
    Iterator<Result> executeQuery(String script, long timeout, TimeUnit timeUnit) throws Exception;

    /**
     * Add vertex with label and properties, for example, there's person vertex with id/name/age properties,
     * you can add person vertex as follows:
     * <code>MaxGraphClient client = ...;
     * Vertex tomVertex = client.addVertex(label, "person", "id", 1L, "name", "tom", "age", 29);</code>
     *
     * @param keyValues The given property list
     * @return The result vertex with given property list
     * @throws Exception The thrown exception
     */
    Vertex addVertex(final Object... keyValues) throws Exception;


    /**
     * Add vertex with label and properties, for example, there's person vertex with id/name/age properties,
     * you can add person vertex as follows:
     * <code>MaxGraphClient client = ...;
     * Vertex tomVertex = client.addVertices(Lists.newArrayList(
     * Lists.newArrayList(label, "person", "id", 1L, "name", "tom", "age", 29),
     * Lists.newArrayList(label, "person", "id", 2L, "name", "jim", "age", 30));</code>
     *
     * @param keyValuesList The given property list
     * @return The result vertex list
     * @throws Exception The thrown exception
     */
    List<Vertex> addVertices(final List<List<Object>> keyValuesList) throws Exception;

    /**
     * Update vertex properties with label and properties, for example, there's person vertex with
     * id/name/age properties, and the id is primary key, you can update person's age to 25 as follows:
     * <code>MaxGraphClient client = ...;
     * Vertex tomVertex = client.updateVertex(label, "person", "id", 1L, "age", 25);</code>
     * Notice: the label and primary key of vertex must exist in the key values, and if there's multiple
     * primary keys such as id1, id2, then the update code should be
     * <code>client.updateVertex(label, "person", "id1", 1L, "id2", 2L, "age", 25)</code>
     *
     * @param keyValues The given property list
     * @return The result vertex with given property list
     * @throws Exception The thrown exception
     */
    Vertex updateVertex(final Object... keyValues) throws Exception;

    /**
     * Delete vertex
     *
     * @param vertex The given vertex
     */
    void deleteVertex(Vertex vertex) throws Exception;

    /**
     * Add edge from src to dst vertex
     *
     * @param srcVertex The src vertex
     * @param dstVertex The dst vertex
     * @param keyValues The key value lsit
     * @return The result edge
     * @throws Exception The thrown exception
     */
    Edge addEdge(Vertex srcVertex, Vertex dstVertex, final Object... keyValues) throws Exception;

    /**
     * Add edge from src to dst vertex
     *
     * @param edgePropsList edge properties list
     * @return The result edge list
     * @throws Exception The thrown exception
     */
    List<Edge> addEdges(List<Triple<Vertex, Vertex, List<Object>>> edgePropsList) throws Exception;

    /**
     * Update edge with given edge id, label and the update property list
     *
     * @param id        The id of edge
     * @param label     The label of edge
     * @param srcVertex The src vertex
     * @param dstVertex The dst vertex
     * @param keyValues The update property list
     * @throws Exception The throw exception
     */
    void updateEdge(final long id, final String label, Vertex srcVertex, Vertex dstVertex, final Object... keyValues) throws Exception;

    /**
     * Update edge with given edge value
     *
     * @param edge      The given edge value
     * @param keyValues THe update property list
     * @throws Exception The thrown exception
     */
    void updateEdge(final Edge edge, final Object... keyValues) throws Exception;

    /**
     * Delete edge
     *
     * @param edge The given edge
     */
    void deleteEdge(Edge edge) throws Exception;

    /**
     * Create vertex type, you can create vertex type as follows:
     * <code>client.createVertexType("person")
     * .addProperty("id", "long")
     * .addProperty("name", "string")
     * .addProperty("age", "int")
     * .primaryKey("id", "name")
     * .execute();</code>
     *
     * @param label The given vertex label
     * @return The vertex schema manager
     */
    ExecuteVertexSchemaManager createVertexType(String label);

    /**
     * Alter vertex/edge type, you can drop/add/update properties as follows:
     * <code>
     * client.alterVertexEdgeType("person")
     * .addProperty("number", "string", "person's number")
     * .dropProperty("age")
     * .updateProperty("name", "string", "person name")
     * .execute();
     * </code>
     *
     * @param label The given vertex/edge type
     * @return The vertex/edge schema manager
     */
    ExecuteAlterVertexEdgeSchemaManager alterVertexEdgeType(String label);

    /**
     * Alter relation in edge type, you can drop/add relatoin as follows:
     * <code>
     * client.alterEdgeRelation("knows")
     * .addRelation("person", "person")
     * .dropRelation("person", "software")
     * .execute();
     * </code>
     *
     * @param label The given edge type
     * @return The edge relation manager
     */
    ExecuteAlterEdgeRelationManager alterEdgeRelation(String label);

    /**
     * Create edge type, you can create edge type as follows:
     * <code>client.createEdgeType("knows")
     * .addProperty("id", "long")
     * .addProperty("weight", "double")
     * .addRelation("person", "person")
     * .addRelation("person", "software")
     * .execute();</code>
     *
     * @param label The given edge label
     * @return The edge schema manager
     */
    ExecuteEdgeSchemaManager createEdgeType(String label);

    /**
     * Delete vertex or edge type
     *
     * @param label The given vertex/edge type
     */
    void dropVertexEdgeType(String label);

    /**
     * Get schema string
     *
     * @return The schema string
     */
    String getSchema();

    /**
     * Create schema from given string
     *
     * @param schema The schema string
     */
    void createSchema(String schema);

    /**
     * Create schema from given input stream
     *
     * @param inputStream The given input stream
     */
    void createSchema(InputStream inputStream);
}
