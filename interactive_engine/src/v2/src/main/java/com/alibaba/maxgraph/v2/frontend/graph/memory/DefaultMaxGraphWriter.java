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
package com.alibaba.maxgraph.v2.frontend.graph.memory;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeRelation;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphProperty;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultVertexType;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default max graph writer for testing
 */
public class DefaultMaxGraphWriter implements MaxGraphWriter {
    private UUID requestId;
    private String sessionId;
    private DefaultMemoryGraph graph;
    private DefaultGraphSchema schema;

    private Map<String, Integer> propNameIdList = Maps.newHashMap();
    private Map<String, Integer> labelNameIdList = Maps.newHashMap();
    private Map<String, Long> labelNameTableList = Maps.newHashMap();
    private int propIndex = 1;
    private int labelIndex = 1;
    private long edgeIndex = 1;
    private long tableIndex = 1;

    public DefaultMaxGraphWriter(UUID requestId,
                                 String sessionId,
                                 DefaultGraphSchema schema,
                                 DefaultMemoryGraph graph) {
        this.requestId = requestId;
        this.sessionId = sessionId;
        this.schema = schema;
        this.graph = graph;
    }

    public DefaultMaxGraphWriter(DefaultGraphSchema schema,
                                 DefaultMemoryGraph graph) {
        this(UUID.randomUUID(), "", schema, graph);
    }

    private int getPropertyId(String propName) {
        if (!propNameIdList.containsKey(propName)) {
            propNameIdList.put(propName, propIndex++);
        }

        return propNameIdList.get(propName);
    }

    private int getLabelId(String label) {
        if (labelNameIdList.containsKey(label)) {
            throw new GraphCreateSchemaException("duplicate label name " + label);
        }
        labelNameIdList.put(label, labelIndex++);
        return labelNameIdList.get(label);
    }

    private long getEdgeId() {
        return edgeIndex++;
    }

    private long getTableId(String label) {
        if (labelNameTableList.containsKey(label)) {
            throw new GraphCreateSchemaException("duplicate label name " + label);
        }
        labelNameTableList.put(label, tableIndex++);
        return labelNameTableList.get(label);
    }

    @Override
    public Future<Integer> createVertexType(String label, List<GraphProperty> propertyList, List<String> primaryKeyList) {
        int labelId = getLabelId(label);
        long tableId = getTableId(label);
        DefaultVertexType vertex = new DefaultVertexType(label,
                labelId,
                propertyList.stream()
                        .map(p -> new DefaultGraphProperty(p.getName(), getPropertyId(p.getName()), p.getDataType()))
                        .collect(Collectors.toList()),
                primaryKeyList,
                0,
                tableId);
        schema.createVertexType(vertex);
        return Futures.immediateFuture(labelId);
    }

    @Override
    public Future<Integer> createEdgeType(String label, List<GraphProperty> propertyList, List<EdgeRelation> relationList) {
        List<GraphProperty> resultPropertyList = propertyList.stream().map(v -> {
            int propId = getPropertyId(v.getName());
            return (GraphProperty) new DefaultGraphProperty(v.getName(), propId, v.getDataType());
        }).collect(Collectors.toList());
        int labelId = getLabelId(label);
        DefaultEdgeType edgeType = new DefaultEdgeType(label, labelId, resultPropertyList, relationList, 0);
        schema.createEdgeType(edgeType);
        return Futures.immediateFuture(labelId);
    }

    @Override
    public Future<Integer> addProperty(String label, GraphProperty property) {
        int propId = getPropertyId(property.getName());
        GraphProperty resultProperty = new DefaultGraphProperty(property.getName(), propId, property.getDataType());
        schema.addProperty(label, resultProperty);
        return Futures.immediateFuture(propId);
    }

    @Override
    public Future<Void> dropProperty(String label, String property) {
        schema.dropProperty(label, property);
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> addEdgeRelation(String edgeLabel, String sourceLabel, String destLabel) {
        try {
            VertexType sourceVertex = (VertexType) this.schema.getSchemaElement(sourceLabel);
            VertexType destVertex = (VertexType) this.schema.getSchemaElement(destLabel);
            EdgeRelation relation = new DefaultEdgeRelation(sourceVertex, destVertex, -1L);
            ((EdgeType) this.schema.getSchemaElement(edgeLabel)).getRelationList().add(relation);
        } catch (GraphElementNotFoundException e) {
            throw new GraphCreateSchemaException("add relation " + sourceLabel + "->" + edgeLabel + "->" + destLabel + " failed", e);
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> dropEdgeRelation(String edgeLabel, String sourceLabel, String destLabel) {
        EdgeType graphEdge;
        try {
            graphEdge = (EdgeType) this.schema.getSchemaElement(edgeLabel);
        } catch (GraphElementNotFoundException e) {
            throw new GraphCreateSchemaException("edge " + edgeLabel + " not exist", e);
        }
        List<EdgeRelation> relationList = graphEdge.getRelationList();
        relationList.removeIf(v -> StringUtils.equals(v.getSource().getLabel(), sourceLabel)
                && StringUtils.equals(v.getTarget().getLabel(), destLabel));
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> dropVertexType(String label) {
        this.schema.dropVertexType(label);
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> dropEdgeType(String label) {
        this.schema.dropEdgeType(label);
        return Futures.immediateFuture(null);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
    }

    @Override
    public Future<Void> commit() {
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<ElementId> insertVertex(String label, Map<String, Object> properties) throws GraphWriteDataException {
        checkNotNull(properties);
        try {
            VertexType graphVertex = (VertexType) this.schema.getSchemaElement(label);
            List<Object> primaryValueList = graphVertex.getPrimaryKeyConstraint()
                    .getPrimaryKeyList()
                    .stream().map(properties::get)
                    .collect(Collectors.toList());
            long id = Objects.hashCode(primaryValueList.toArray());
            ElementId vertexId = new CompositeId(id, graphVertex.getLabelId());
            this.graph.addVertex(vertexId, properties);
            return Futures.immediateFuture(vertexId);
        } catch (GraphElementNotFoundException e) {
            throw new GraphWriteDataException("insert vertex fail", e);
        }
    }

    @Override
    public Future<List<ElementId>> insertVertices(List<Pair<String, Map<String, Object>>> vertices) throws GraphWriteDataException {
        List<ElementId> vertexIds = Lists.newArrayList();
        for (Pair<String, Map<String, Object>> pair : vertices) {
            try {
                vertexIds.add(insertVertex(pair.getLeft(), pair.getRight()).get());
            } catch (InterruptedException | ExecutionException e) {
                throw new GraphWriteDataException("insert vertex fail", e);
            }
        }
        return Futures.immediateFuture(vertexIds);
    }

    @Override
    public Future<Void> updateVertexProperties(ElementId vertexId, Map<String, Object> properties) throws GraphWriteDataException {
        Map<String, Object> vertexProperties = this.graph.getVertexProperties(vertexId);
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() == null) {
                vertexProperties.remove(entry.getKey());
            } else {
                vertexProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> deleteVertex(ElementId vertexId) {
        this.graph.deleteVertex(vertexId);
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> deleteVertices(Set<ElementId> vertexIds) {
        if (null != vertexIds) {
            for (ElementId vertexId : vertexIds) {
                deleteVertex(vertexId);
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<ElementId> insertEdge(ElementId srcId, ElementId dstId, String label, Map<String, Object> properties) {
        checkNotNull(properties);
        int labelId = this.schema.getSchemaElement(label).getLabelId();
        long edgeId = this.getEdgeId();
        ElementId eid = new CompositeId(edgeId, labelId);
        this.graph.addEdge(srcId, dstId, eid, properties);
        return Futures.immediateFuture(eid);
    }

    @Override
    public Future<Void> updateEdgeProperties(ElementId srcId, ElementId destId, ElementId edgeId, Map<String, Object> properties) throws GraphWriteDataException {
        Map<String, Object> edgeProperties = this.graph.getEdge(edgeId).getProperties();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() == null) {
                edgeProperties.remove(entry.getKey());
            } else {
                edgeProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> deleteEdge(ElementId srcId, ElementId dstId, ElementId edgeId) {
        this.graph.deleteEdge(srcId, dstId, edgeId);
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<Void> deleteEdges(List<Triple<ElementId, ElementId, ElementId>> edgeList) {
        for (Triple<ElementId, ElementId, ElementId> triple : edgeList) {
            this.deleteEdge(triple.getLeft(), triple.getMiddle(), triple.getRight());
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public Future<List<ElementId>> insertEdges(List<Triple<Pair<ElementId, ElementId>, String, Map<String, Object>>> edges) {
        List<ElementId> edgeIdList = Lists.newArrayList();
        for (Triple<Pair<ElementId, ElementId>, String, Map<String, Object>> triple : edges) {
            try {
                edgeIdList.add(this.insertEdge(triple.getLeft().getLeft(), triple.getLeft().getRight(), triple.getMiddle(), triple.getRight()).get());
            } catch (Exception e) {
                throw new GraphWriteDataException("insert edges fail", e);
            }
        }
        return Futures.immediateFuture(edgeIdList);
    }

}
