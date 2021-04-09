package com.alibaba.maxgraph.v2.common.frontend.api.graph;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphQueryDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphReader;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Set;

/**
 * MaxGraph reader API which will access graph store to fetch vertices/edges/properties.
 * Different graph store should implements the {@link MaxGraphReader}, such as {@link DefaultMaxGraphReader}(not ready) is implemented
 * as a reader to access local memory store for testing
 */
public interface MaxGraphReader {
    /**
     * Get vertex with given vertex id
     *
     * @param vertexId The given vertex id
     * @return The result vertex
     * @throws GraphQueryDataException The thrown exception
     */
    Vertex getVertex(ElementId vertexId) throws GraphQueryDataException;

    /**
     * Get vertices with given ids
     *
     * @param vertexIds The given id set
     * @return The result vertices
     * @throws GraphQueryDataException The thrown exception
     */
    Iterator<Vertex> getVertices(Set<ElementId> vertexIds) throws GraphQueryDataException;

    /**
     * Get vertices with given ids and direction
     *
     * @param vertexIds  The given id set
     * @param direction  The given direction
     * @param edgeLabels The given edge labels
     * @return The result vertices
     * @throws GraphQueryDataException The thrown exception
     */
    Iterator<Vertex> getVertices(Set<ElementId> vertexIds, Direction direction, String... edgeLabels) throws GraphQueryDataException;

    /**
     * Scan vertices
     *
     * @param vertexLabels The given vertex labels
     * @return The result vertices
     * @throws GraphQueryDataException The thrown exception
     */
    Iterator<Vertex> scanVertices(String... vertexLabels) throws GraphQueryDataException;

    /**
     * Get edges with given edge id
     *
     * @param edgeIdList The given edge id
     * @return The result edges
     */
    Iterator<Edge> getEdges(Set<ElementId> edgeIdList) throws GraphQueryDataException;

    /**
     * Get edges with vertex and direction
     *
     * @param vertexId   The given vertex id
     * @param direction  The direction
     * @param edgeLabels The given edge labels
     * @return The result edges
     * @throws GraphQueryDataException The thrown exception
     */
    Iterator<Edge> getEdges(ElementId vertexId, Direction direction, String... edgeLabels) throws GraphQueryDataException;

    /**
     * Scan edges
     *
     * @param edgeLabels The given edge labels
     * @return The result edges
     * @throws GraphQueryDataException The thrown exception
     */
    Iterator<Edge> scanEdges(String... edgeLabels) throws GraphQueryDataException;
}
