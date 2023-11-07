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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.server.ExportException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlogueSchema {
    private Graph<Integer, EdgeTypeId> schemaGraph;
    private HashMap<Integer, Double> vertexTypeCardinality;
    private HashMap<EdgeTypeId, Double> edgeTypeCardinality;

    public GlogueSchema() {
        this.schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
    }

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

    public Graph<Integer, EdgeTypeId> getSchemaGraph() {
        return this.schemaGraph;
    }

    public Double getVertexTypeCardinality(Integer vertexType) {
        return this.vertexTypeCardinality.get(vertexType);
    }

    public Double getEdgeTypeCardinality(EdgeTypeId edgeType) {
        return this.edgeTypeCardinality.get(edgeType);
    }

    // modern graph schema
    // person: label 0, statistics 3;
    // software: label 1, statistics 4;
    // person-knows->person: label 0, statistics 5;
    // person-created->software: label 1, statistics 6;
    public GlogueSchema DefaultGraphSchema() {
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();

        DefaultGraphVertex person =
                new DefaultGraphVertex(0, "person", List.of(), List.of(), 0, -1);
        DefaultGraphVertex software =
                new DefaultGraphVertex(1, "software", List.of(), List.of(), 0, -1);
        vertexList.put("person", person);
        vertexList.put("software", software);
        DefaultEdgeRelation knowsRelation = new DefaultEdgeRelation(person, person);
        DefaultGraphEdge knows =
                new DefaultGraphEdge(0, "knows", List.of(), List.of(knowsRelation), 0);
        DefaultEdgeRelation createdRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge created =
                new DefaultGraphEdge(1, "created", List.of(), List.of(createdRelation), 0);
        edgeList.put("knows", knows);
        edgeList.put("created", created);

        DefaultGraphSchema graphSchema =
                new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        vertexTypeCardinality.put(0, 3.0);
        vertexTypeCardinality.put(1, 4.0);

        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        edgeTypeCardinality.put(new EdgeTypeId(0, 0, 0), 5.0);
        edgeTypeCardinality.put(new EdgeTypeId(0, 1, 1), 6.0);

        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        System.out.println("glogue schema: " + g);
        return g;
    }

    // person-created->software, person-uses->software
    public GlogueSchema DefaultGraphSchema2() {
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();
        DefaultGraphVertex person =
                new DefaultGraphVertex(11, "person", List.of(), List.of(), 0, -1);
        DefaultGraphVertex software =
                new DefaultGraphVertex(22, "software", List.of(), List.of(), 0, -1);
        vertexList.put("person", person);
        vertexList.put("software", software);
        DefaultEdgeRelation usesRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge uses =
                new DefaultGraphEdge(1111, "uses", List.of(), List.of(usesRelation), 0);
        DefaultEdgeRelation createdRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge created =
                new DefaultGraphEdge(1112, "created", List.of(), List.of(createdRelation), 0);
        edgeList.put("uses", uses);
        edgeList.put("created", created);

        DefaultGraphSchema graphSchema =
                new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        vertexTypeCardinality.put(11, 4.0);
        vertexTypeCardinality.put(22, 2.0);

        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        edgeTypeCardinality.put(new EdgeTypeId(11, 22, 1111), 2.0);
        edgeTypeCardinality.put(new EdgeTypeId(11, 22, 1112), 4.0);

        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        return g;
    }

    public GlogueSchema SchemaFromFile(String schemaPath) {
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();

        // read schema from file
        File file = new File(schemaPath);

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // seperate by comma
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
            bufferedReader.close();
        } catch (NumberFormatException | IOException e) {
            e.printStackTrace();
        }

        DefaultGraphSchema graphSchema =
                new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        return g;
    }

    public static void main(String[] args) throws URISyntaxException, ExportException {
        GlogueSchema g =
                new GlogueSchema()
                        .SchemaFromFile(
                                "/workspaces/GraphScope/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/common/ir/rel/metadata/schema/resource/ldbc1_statistics.txt");
        for (Integer vertexType : g.getVertexTypes()) {
            System.out.println(
                    "cardinality of " + vertexType + ": " + g.getVertexTypeCardinality(vertexType));
        }
        for (EdgeTypeId edgeType : g.getEdgeTypes()) {
            System.out.println(
                    "cardinality of " + edgeType + ": " + g.getEdgeTypeCardinality(edgeType));
        }
        System.out.println("edges of place");
        g.getAdjEdgeTypes(0).forEach(System.out::println);
        System.out.println("edges of person");
        g.getAdjEdgeTypes(1).forEach(System.out::println);
    }
}
