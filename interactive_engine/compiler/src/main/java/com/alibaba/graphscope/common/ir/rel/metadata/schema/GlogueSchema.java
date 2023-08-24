package com.alibaba.graphscope.common.ir.rel.metadata.schema;

import java.net.URISyntaxException;
import java.rmi.server.ExportException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Triplet;
import org.jgrapht.*;
import org.jgrapht.graph.*;

import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultEdgeRelation;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphEdge;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphSchema;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphVertex;
import com.google.common.collect.Maps;

public class GlogueSchema {
    private Graph<Integer, EdgeTypeId> schemaGraph;
    private HashMap<Integer, Double> vertexTypeCardinality;
    private HashMap<EdgeTypeId, Double> edgeTypeCardinality;

    public GlogueSchema() {
        this.schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
    }

    public GlogueSchema(GraphSchema graphSchema, HashMap<Integer, Double> vertexTypeCardinality,
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
    public GlogueSchema DefaultGraphSchema() {
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();

        
        DefaultGraphVertex person = new DefaultGraphVertex(11, "person",  List.of(), List.of("name"), 0, -1);
        DefaultGraphVertex software = new DefaultGraphVertex(22, "software", List.of(), List.of("name"), 0, -1);
        vertexList.put("person", person);
        vertexList.put("software", software);
        DefaultEdgeRelation knowsRelation = new DefaultEdgeRelation(person, person);
        DefaultGraphEdge knows = new DefaultGraphEdge(1111, "knows", List.of(), List.of(knowsRelation), 0);
        DefaultEdgeRelation createdRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge created = new DefaultGraphEdge(1112, "created", List.of(), List.of(createdRelation), 0);
        edgeList.put("knows", knows);
        edgeList.put("created", created);

        DefaultGraphSchema graphSchema = new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        vertexTypeCardinality.put(11, 4.0);
        vertexTypeCardinality.put(22, 2.0);

        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        edgeTypeCardinality.put(new EdgeTypeId(11, 11, 1111), 2.0);
        edgeTypeCardinality.put(new EdgeTypeId(11, 22, 1112), 4.0);

        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        return g;
    }
    
    // person-created->software, person-uses->software
    public GlogueSchema DefaultGraphSchema2() {
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        Map<String, GraphEdge> edgeList = Maps.newHashMap();
        DefaultGraphVertex person = new DefaultGraphVertex(11, "person",  List.of(), List.of("name"), 0, -1);
        DefaultGraphVertex software = new DefaultGraphVertex(22, "software", List.of(), List.of("name"), 0, -1);
        vertexList.put("person", person);
        vertexList.put("software", software);
        DefaultEdgeRelation usesRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge uses = new DefaultGraphEdge(1111, "uses", List.of(), List.of(usesRelation), 0);
        DefaultEdgeRelation createdRelation = new DefaultEdgeRelation(person, software);
        DefaultGraphEdge created = new DefaultGraphEdge(1112, "created", List.of(), List.of(createdRelation), 0);
        edgeList.put("uses", uses);
        edgeList.put("created", created);

    
        DefaultGraphSchema graphSchema = new DefaultGraphSchema(vertexList, edgeList, Maps.newHashMap());
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        vertexTypeCardinality.put(11, 4.0);
        vertexTypeCardinality.put(22, 2.0);

        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        edgeTypeCardinality.put(new EdgeTypeId(11, 22, 1111), 2.0);
        edgeTypeCardinality.put(new EdgeTypeId(11, 22, 1112), 4.0);

        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        return g;
    }

    public static void main(String[] args) throws URISyntaxException, ExportException {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        System.out.println("vertices");
        g.getVertexTypes().forEach(System.out::println);
        System.out.println("edges");
        g.getEdgeTypes().forEach(System.out::println);
        System.out.println("edges of person");
        g.getAdjEdgeTypes(11).forEach(System.out::println);
        System.out.println("edges of software");
        g.getAdjEdgeTypes(22).forEach(System.out::println);
    }
}
