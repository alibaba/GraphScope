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
package com.alibaba.maxgraph.structure.graph;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.Edge;
import com.alibaba.maxgraph.structure.Vertex;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MaxGraph extends Closeable {


    Iterator<Vertex> getVertex(Set<ElementId> id);

    Iterator<Vertex> getVertex(String... label);

    Vertex addVertex(String label, Map<String, Object> properties);

    List<Vertex> addVertices(List<Pair<String, Map<String, Object>>> vertexList);

    void deleteVertex(CompositeId vertexId);

    Iterator<Edge> getEdges(Vertex v, Direction direction, String... label);

    Iterator<Edge> getEdges(Set<Vertex> v, Direction direction, String... label);

    Iterator<Edge> getEdges(String... label);

    Edge addEdge(String label, Vertex src, Vertex dst, Map<String, Object> properties);

    List<Edge> addEdges(List<Triple<String, Pair<Vertex, Vertex>, Map<String, Object>>> edgeList);

    void deleteEdge(String label, long edgeId, ElementId srcId, ElementId dstId);

    void updateEdge(Vertex src, Vertex dst, String label, long edgeId, Map<String, Object> propertyList);

    GraphSchema getSchema();

    void refresh() throws Exception;

    Map<Integer, Set<ElementId>> partition(Set<ElementId> elementIds);
}
