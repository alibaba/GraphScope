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
import com.alibaba.graphscope.groot.common.schema.api.*;

import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class GlogueSchema {
    private Graph<Integer, EdgeTypeId> schemaGraph;
    private HashMap<Integer, Double> vertexTypeCardinality;
    private HashMap<EdgeTypeId, Double> edgeTypeCardinality;

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

    // build a default GlogueSchema from GraphSchema by assuming all vertex and edge types have the same cardinality 1.0
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
    }

    public GlogueSchema(GraphSchema graphSchema, GraphStatistics statistics) {
        schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
        vertexTypeCardinality = new HashMap<Integer, Double>();
        edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        for (GraphVertex vertex : graphSchema.getVertexList()) {
            schemaGraph.addVertex(vertex.getLabelId());
            Long vertexTypeCount = statistics.getVertexTypeCount(vertex.getLabelId());
            if (vertexTypeCount == null) {
                throw new IllegalArgumentException(
                        "Vertex type count not found for vertex type: " + vertex.getLabelId());
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
                } else {
                    edgeTypeCardinality.put(edgeType, edgeTypeCount.doubleValue());
                }
            }
        }
    }

    public static GlogueSchema fromMeta(IrMetaStats irMeta) {
        return new GlogueSchema(irMeta.getSchema(), irMeta.getStatistics());
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
            return 0.0;
        } else {
            return cardinality;
        }
    }

    public Double getEdgeTypeCardinality(EdgeTypeId edgeType) {
        Double cardinality = this.edgeTypeCardinality.get(edgeType);
        if (cardinality == null) {
            return 0.0;
        } else {
            return cardinality;
        }
    }
}
