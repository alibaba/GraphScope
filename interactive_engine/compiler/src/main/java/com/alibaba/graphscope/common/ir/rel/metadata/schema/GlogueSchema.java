/*
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel.metadata.schema;

import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;

import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class GlogueSchema {
    private Graph<Integer, EdgeTypeId> schemaGraph;
    private HashMap<Integer, Double> vertexTypeCardinality;
    private HashMap<EdgeTypeId, Double> edgeTypeCardinality;
    private static Logger logger = LoggerFactory.getLogger(GlogueSchema.class);

    public GlogueSchema(
            GraphSchema graphSchema,
            HashMap<Integer, Double> vertexTypeCardinality,
            HashMap<EdgeTypeId, Double> edgeTypeCardinality) {
        this.schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
        for (GraphVertex vertex : graphSchema.getVertexList()) {
            this.schemaGraph.addVertex(vertex.getLabelId());
        }
        for (GraphEdge edge : graphSchema.getEdgeList()) {
            for (EdgeRelation relation : edge.getRelationList()) {
                int sourceType = relation.getSource().getLabelId();
                int targetType = relation.getTarget().getLabelId();
                EdgeTypeId edgeType = new EdgeTypeId(sourceType, targetType, edge.getLabelId());
                this.schemaGraph.addEdge(sourceType, targetType, edgeType);
            }
        }

        this.vertexTypeCardinality = vertexTypeCardinality;
        this.edgeTypeCardinality = edgeTypeCardinality;
    }

    public GlogueSchema(GraphSchema graphSchema) {
        schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
        vertexTypeCardinality = new HashMap<Integer, Double>();
        edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        for (GraphVertex vertex : graphSchema.getVertexList()) {
            schemaGraph.addVertex(vertex.getLabelId());
            vertexTypeCardinality.put(vertex.getLabelId(), 1.0);
        }
        for (GraphEdge edge : graphSchema.getEdgeList()) {
            for (EdgeRelation relation : edge.getRelationList()) {
                int sourceType = relation.getSource().getLabelId();
                int targetType = relation.getTarget().getLabelId();
                EdgeTypeId edgeType = new EdgeTypeId(sourceType, targetType, edge.getLabelId());
                schemaGraph.addEdge(sourceType, targetType, edgeType);
                edgeTypeCardinality.put(edgeType, 1.0);
            }
        }
        logger.info("GlogueSchema created with default cardinality 1.0: {}", this);
    }

    public GlogueSchema(GraphSchema graphSchema, GraphStatistics statistics) {
        logger.info(
                "Creating GlogueSchema with statistics, vertex count: {}, edge count: {}",
                statistics.getVertexCount(),
                statistics.getEdgeCount());

        schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
        vertexTypeCardinality = new HashMap<Integer, Double>();
        edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        for (GraphVertex vertex : graphSchema.getVertexList()) {
            schemaGraph.addVertex(vertex.getLabelId());
            Long vertexTypeCount = statistics.getVertexTypeCount(vertex.getLabelId());
            if (vertexTypeCount == null) {
                throw new IllegalArgumentException(
                        "Vertex type count not found for vertex type: " + vertex.getLabelId());
            } else if (vertexTypeCount == 0) {
                vertexTypeCardinality.put(vertex.getLabelId(), 1.0);
            } else {
                vertexTypeCardinality.put(vertex.getLabelId(), vertexTypeCount.doubleValue());
            }
        }
        for (GraphEdge edge : graphSchema.getEdgeList()) {
            for (EdgeRelation relation : edge.getRelationList()) {
                int sourceType = relation.getSource().getLabelId();
                int targetType = relation.getTarget().getLabelId();
                EdgeTypeId edgeType = new EdgeTypeId(sourceType, targetType, edge.getLabelId());
                schemaGraph.addEdge(sourceType, targetType, edgeType);
                Long edgeTypeCount =
                        statistics.getEdgeTypeCount(
                                Optional.of(sourceType),
                                Optional.of(edge.getLabelId()),
                                Optional.of(targetType));
                if (edgeTypeCount == null) {
                    throw new IllegalArgumentException(
                            "Edge type count not found for edge type: " + edge.getLabelId());
                } else if (edgeTypeCount == 0) {
                    edgeTypeCardinality.put(edgeType, 1.0);
                } else {
                    edgeTypeCardinality.put(edgeType, edgeTypeCount.doubleValue());
                }
            }
        }
        logger.info("GlogueSchema created with statistics: {}", this);
    }

    public static GlogueSchema fromMeta(IrMetaStats irMeta) {
        if (irMeta.getStatistics() == null) {
            // build a default GlogueSchema by assuming all vertex and edge types have the same
            // cardinality 1.0
            return new GlogueSchema(irMeta.getSchema());
        } else {
            return new GlogueSchema(irMeta.getSchema(), irMeta.getStatistics());
        }
    }

    public Double getLabelConstraintsDeltaCost(PatternEdge edge, PatternVertex target) {
        PatternDirection direction = Utils.getExtendDirection(edge, target);
        double deltaCost = 0.0d;
        if (direction != PatternDirection.IN) {
            deltaCost += getLabelConstraintsDeltaCost(edge, PatternDirection.OUT);
        }
        if (direction != PatternDirection.OUT) {
            deltaCost += getLabelConstraintsDeltaCost(edge, PatternDirection.IN);
        }
        return deltaCost;
    }

    private Double getLabelConstraintsDeltaCost(PatternEdge edge, PatternDirection direction) {
        AtomicDouble deltaCost = new AtomicDouble(0.0d);
        Set<EdgeTypeId> visited = Sets.newHashSet();
        edge.getEdgeTypeIds()
                .forEach(
                        edgeTypeId -> {
                            EdgeTypeId key =
                                    (direction == PatternDirection.OUT)
                                            ? new EdgeTypeId(
                                                    edgeTypeId.getSrcLabelId(),
                                                    edgeTypeId.getEdgeLabelId(),
                                                    -1)
                                            : new EdgeTypeId(
                                                    -1,
                                                    edgeTypeId.getEdgeLabelId(),
                                                    edgeTypeId.getDstLabelId());
                            if (visited.contains(key)) {
                                return;
                            }
                            visited.add(key);
                            List<EdgeTypeId> candidates = Lists.newArrayList();
                            edgeTypeCardinality.forEach(
                                    (k, v) -> {
                                        switch (direction) {
                                            case OUT:
                                                if (edgeTypeId.getSrcLabelId() == k.getSrcLabelId()
                                                        && edgeTypeId.getEdgeLabelId()
                                                                == k.getEdgeLabelId()) {
                                                    candidates.add(k);
                                                }
                                                break;
                                            case IN:
                                                if (edgeTypeId.getDstLabelId() == k.getDstLabelId()
                                                        && edgeTypeId.getEdgeLabelId()
                                                                == k.getEdgeLabelId()) {
                                                    candidates.add(k);
                                                }
                                                break;
                                            default:
                                        }
                                    });
                            if (!edge.getEdgeTypeIds().containsAll(candidates)) {
                                double deltaSum = 0.0d;
                                for (EdgeTypeId candidate : candidates) {
                                    deltaSum += getEdgeTypeCardinality(candidate);
                                }
                                deltaCost.addAndGet(deltaSum);
                            }
                        });
        return deltaCost.get();
    }

    public List<Integer> getVertexTypes() {
        return List.copyOf(this.schemaGraph.vertexSet());
    }

    public List<EdgeTypeId> getEdgeTypes() {
        return List.copyOf(this.schemaGraph.edgeSet());
    }

    public List<EdgeTypeId> getAdjEdgeTypes(Integer source) {
        return List.copyOf(this.schemaGraph.edgesOf(source));
    }

    public List<EdgeTypeId> getEdgeTypes(Integer source, Integer target) {
        return List.copyOf(this.schemaGraph.getAllEdges(source, target));
    }

    public Double getVertexTypeCardinality(Integer vertexType) {
        Double cardinality = this.vertexTypeCardinality.get(vertexType);
        if (cardinality == null) {
            logger.debug(
                    "Vertex type {} not found in schema, assuming cardinality 1.0", vertexType);
            return 1.0;
        } else {
            return cardinality;
        }
    }

    public Double getEdgeTypeCardinality(EdgeTypeId edgeType) {
        Double cardinality = this.edgeTypeCardinality.get(edgeType);
        if (cardinality == null) {
            logger.debug("Edge type {} not found in schema, assuming cardinality 1.0", edgeType);
            return 1.0;
        } else {
            return cardinality;
        }
    }

    @Override
    public String toString() {
        String s = "GlogueSchema:\n";
        s += "VertexTypes:\n";
        for (Integer v : this.schemaGraph.vertexSet()) {
            s += v + " " + this.vertexTypeCardinality.get(v) + "\n";
        }
        s += "\nEdgeTypes:\n";
        for (EdgeTypeId e : this.schemaGraph.edgeSet()) {
            s += e.toString() + " " + this.edgeTypeCardinality.get(e) + "\n";
        }
        return s;
    }
}
