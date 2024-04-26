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

import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultEdgeRelation;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphEdge;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphSchema;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphVertex;
import com.google.common.collect.Maps;

import org.jgrapht.*;
import org.jgrapht.graph.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static GlogueSchema fromFile(String schemaPath) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(schemaPath))) {
            Map<String, GraphVertex> vertexList = Maps.newHashMap();
            Map<String, GraphEdge> edgeList = Maps.newHashMap();
            HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
            HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // separate by comma
                String[] parts = line.split(",");
                // vertex
                if (parts[0].equals("v")) {
                    // v, 0, person, 3
                    // v, 1, software, 4
                    int labelId = Integer.parseInt(parts[1]);
                    String label = parts[2];
                    Double statistics = Double.parseDouble(parts[3]);
                    DefaultGraphVertex vertex =
                            new DefaultGraphVertex(labelId, label, List.of(), List.of(), 0, -1);
                    vertexList.put(label, vertex);
                    vertexTypeCardinality.put(labelId, statistics);
                }
                // edge
                else if (parts[0].equals("e")) {
                    // e, 0, knows, person, person, 5
                    // e, 1, created, person, software, 6
                    int labelId = Integer.parseInt(parts[1]);
                    String label = parts[2];
                    GraphVertex srcGraphVertex = vertexList.get(parts[3]);
                    GraphVertex dstGraphVertex = vertexList.get(parts[4]);
                    Double statistics = Double.parseDouble(parts[5]);
                    int srcLabelId = srcGraphVertex.getLabelId();
                    int dstLabelId = dstGraphVertex.getLabelId();
                    DefaultEdgeRelation relation =
                            new DefaultEdgeRelation(srcGraphVertex, dstGraphVertex);
                    DefaultGraphEdge edge =
                            new DefaultGraphEdge(labelId, label, List.of(), List.of(relation), 0);
                    String edgeLabel = srcLabelId + label + dstLabelId;
                    edgeList.put(edgeLabel, edge);
                    edgeTypeCardinality.put(
                            new EdgeTypeId(srcLabelId, dstLabelId, labelId), statistics);
                }
            }
            DefaultGraphSchema graphSchema =
                    new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
            return new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        }
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
