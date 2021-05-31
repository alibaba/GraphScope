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
package com.alibaba.maxgraph.v2.common.frontend.api.graph;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * MaxGraph writer api which will manage the schema and write vertices/edges/properties for the graph.
 * Different graph store should implements the {@link MaxGraphWriter}, such as {@link DefaultMaxGraphWriter}(not ready) is implemented
 * as a writer to access local schema and memory store for testing
 */
public interface MaxGraphWriter {
    /**
     * Create vertex type in schema
     *
     * @param label          The given label
     * @param propertyList   The given property list, the id in GraphProperty will be 0
     * @param primaryKeyList The primary key list for vertex
     * @return The vertex label id future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Integer> createVertexType(String label,
                                     List<GraphProperty> propertyList,
                                     List<String> primaryKeyList) throws GraphCreateSchemaException;

    /**
     * Create edge type in schema
     *
     * @param label        The given label
     * @param propertyList The given property list, the id in GraphProperty will be 0
     * @param relationList The relation list in the schema
     * @return The edge label id future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Integer> createEdgeType(String label,
                                   List<GraphProperty> propertyList,
                                   List<EdgeRelation> relationList) throws GraphCreateSchemaException;

    /**
     * Add property in vertex/edge
     *
     * @param label    The given label
     * @param property The given property added to vertex/edge, the id in GraphProperty will be 0
     * @return The property id future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Integer> addProperty(String label,
                                GraphProperty property) throws GraphCreateSchemaException;

    /**
     * Delete given property in given label
     *
     * @param label    The given label
     * @param property The given property
     * @return The result future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Void> dropProperty(String label, String property) throws GraphCreateSchemaException;

    /**
     * Add edge relation in given edge
     *
     * @param edgeLabel   The edge label
     * @param sourceLabel The source label
     * @param destLabel   The dest label
     * @return The result future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Void> addEdgeRelation(String edgeLabel, String sourceLabel, String destLabel) throws GraphCreateSchemaException;

    /**
     * Delete given edge relation
     *
     * @param edgeLabel   The edge label
     * @param sourceLabel The source label
     * @param destLabel   The dest label
     * @return The result future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Void> dropEdgeRelation(String edgeLabel, String sourceLabel, String destLabel) throws GraphCreateSchemaException;

    /**
     * Drop the vertex type with given label
     *
     * @param label The given label
     * @return The result future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Void> dropVertexType(String label) throws GraphCreateSchemaException;

    /**
     * Drop the edge type with given label id
     *
     * @param label The given label
     * @return The result future
     * @throws GraphCreateSchemaException The thrown exception
     */
    Future<Void> dropEdgeType(String label) throws GraphCreateSchemaException;

    /**
     * Set auto commit flag for writing vertices/edges to store, the auto commit flag should be true by default
     *
     * @param autoCommit The given auto commit flag
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Commit all batched vertices and edges to the store
     *
     * @return The result future
     */
    Future<Void> commit();

    /**
     * Insert override vertex to store and return the vertex id
     *
     * @param label      The label of vertex
     * @param properties The properties of vertex
     * @return The vertex id future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<ElementId> insertVertex(String label, Map<String, Object> properties) throws GraphWriteDataException;

    /**
     * Add vertices to store and return the vertex id list,
     * the struct in pair is Pair<int labelId, Map<String, Object> properties>>
     *
     * @param vertices The given vertices
     * @return The vertex id list future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<List<ElementId>> insertVertices(List<Pair<String, Map<String, Object>>> vertices) throws GraphWriteDataException;

    /**
     * Merge the given properties to the given vertex and throw exception when the vertex is not exist.
     * The null value in properties means the property should be removed from the given vertex
     *
     * @param vertexId   The given vertex
     * @param properties The given properties
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> updateVertexProperties(ElementId vertexId, Map<String, Object> properties) throws GraphWriteDataException;

    /**
     * Delete vertex with given vertexId
     *
     * @param vertexId The given vertexId
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> deleteVertex(ElementId vertexId) throws GraphWriteDataException;

    /**
     * Delete the vertices with given vertexId list
     *
     * @param vertexIds The given vertexId list
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> deleteVertices(Set<ElementId> vertexIds) throws GraphWriteDataException;

    /**
     * Add edge to store and return the edge id
     *
     * @param srcId      The source vertex
     * @param destId     The dst vertex
     * @param label      The given label
     * @param properties The properties
     * @return The edge id future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<ElementId> insertEdge(ElementId srcId, ElementId destId, String label, Map<String, Object> properties) throws GraphWriteDataException;

    /**
     * Merge the given properties to the given edge and throw exception when the edge is not exist.
     * The null value in properties means the property should be removed from the given edge
     *
     * @param srcId      The given source vertex
     * @param destId     The given dst vertex
     * @param edgeId     The given edge id
     * @param properties The given properties
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> updateEdgeProperties(ElementId srcId, ElementId destId, ElementId edgeId, Map<String, Object> properties) throws GraphWriteDataException;

    /**
     * Delete given edge
     *
     * @param srcId  The given source vertexId
     * @param destId The given dst vertexId
     * @param edgeId The given edge id
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> deleteEdge(ElementId srcId, ElementId destId, ElementId edgeId) throws GraphWriteDataException;

    /**
     * Delete the given edges
     *
     * @param edgeList The edge list with <srcId, destId, edgeId>
     * @return The result future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<Void> deleteEdges(List<Triple<ElementId, ElementId, ElementId>> edgeList) throws GraphWriteDataException;

    /**
     * Add edges to store and return the edge id list, the struct
     * in pair is Triple<Pair<source id, dest id>, String edgeLabel, Map<String, Object> properties>
     *
     * @param edges The given edges
     * @return The edge id list future
     * @throws GraphWriteDataException The thrown exception
     */
    Future<List<ElementId>> insertEdges(List<Triple<Pair<ElementId, ElementId>, String, Map<String, Object>>> edges) throws GraphWriteDataException;
}
