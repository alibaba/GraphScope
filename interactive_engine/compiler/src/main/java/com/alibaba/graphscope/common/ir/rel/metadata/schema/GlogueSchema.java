package com.alibaba.graphscope.common.ir.rel.metadata.schema;

import java.net.URISyntaxException;
import java.rmi.server.ExportException;
import java.util.List;

import org.javatuples.Triplet;
import org.jgrapht.*;
import org.jgrapht.graph.*;

import com.alibaba.graphscope.compiler.api.schema.EdgeRelation;
import com.alibaba.graphscope.compiler.api.schema.GraphEdge;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.sdkcommon.schema.mapper.DefaultEdgeRelation;
import com.alibaba.graphscope.sdkcommon.schema.mapper.DefaultGraphEdge;
import com.alibaba.graphscope.sdkcommon.schema.mapper.DefaultGraphSchema;
import com.alibaba.graphscope.sdkcommon.schema.mapper.DefaultGraphVertex;

public class GlogueSchema {
    private Graph<Integer, EdgeTypeId> schemaGraph;

    public GlogueSchema() {
        this.schemaGraph = new DirectedPseudograph<Integer, EdgeTypeId>(EdgeTypeId.class);
    }

    public GlogueSchema(GraphSchema graphSchema) {
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

    public List<EdgeTypeId> getBiDirEdgeTypes(Integer source, Integer target) {
        return List.copyOf(
                this.schemaGraph.getAllEdges(source, target).addAll(this.schemaGraph.getAllEdges(target, source)));
    }

    public Graph<Integer, EdgeTypeId> getSchemaGraph() {
        return this.schemaGraph;
    }

    public GlogueSchema DefaultGraphSchema() {
        DefaultGraphSchema graphSchema = new DefaultGraphSchema();
        DefaultGraphVertex person = new DefaultGraphVertex("person", 11, List.of(), List.of("name"), 0, -1);
        DefaultGraphVertex software = new DefaultGraphVertex("software", 22, List.of(), List.of("name"), 0, -1);
        graphSchema.createVertexType(person);
        graphSchema.createVertexType(software);
        DefaultEdgeRelation knowsRelation = new DefaultEdgeRelation(person, person);
        graphSchema.createEdgeType(new DefaultGraphEdge("knows", 1111, List.of(), List.of(knowsRelation), 0));
        DefaultEdgeRelation createdRelation = new DefaultEdgeRelation(person, software);
        graphSchema.createEdgeType(new DefaultGraphEdge("created", 1112, List.of(), List.of(createdRelation), 0));
        GlogueSchema g = new GlogueSchema(graphSchema);
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
