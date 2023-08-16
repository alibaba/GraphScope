package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;

import com.alibaba.graphscope.common.ir.rel.graph.pattern.PatternCode;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Glogue {
    // the topology of GLogue graph
    private Graph<Pattern, DefaultEdge> gLogueGraph;
    // the index for pattern count query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern count
    private HashMap<PatternCode, Double> patternCount;
    // the index for pattern locate query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern locate
    private HashMap<PatternCode, Integer> patternLocate;

    protected Glogue() {
        this.gLogueGraph = new DirectedPseudograph<Pattern, DefaultEdge>(DefaultEdge.class);
        this.patternCount = new HashMap<PatternCode, Double>();
        this.patternLocate = new HashMap<PatternCode, Integer>();
    }

    // Construct NewGlogue from a graph schema with default max pattern size = 3
    public Glogue create(GlogueSchema schema) {
        return this.create(schema, 3);
    }

    // Construct NewGlogue from a graph schema with given max pattern size
    public Glogue create(GlogueSchema schema, int maxPatternSize) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        // add all vertices as patterns
        for (Integer vertexTypeId : schema.getVertexTypes()) {
            PatternVertex vertex = new PatternVertex(vertexTypeId);
            Pattern new_pattern = new Pattern(vertex);
            this.addPattern(new_pattern);
            patternQueue.add(new_pattern);
        }

        System.out.println("init glogue " + this);
        System.out.println("init patternQueue " + patternQueue.toString());

        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            if (pattern.size() >= maxPatternSize) {
                continue;
            }
            System.out.println("~~~~~~~~pop pattern in queue~~~~~~~~~~");
            System.out.println("original pattern " + pattern.toString());
            List<ExtendStep> extendSteps = pattern.getExtendSteps(schema);
            for (ExtendStep extendStep : extendSteps) {
                System.out.println(extendStep);
                Pattern newPattern = pattern.extend(extendStep);
                System.out.println("new pattern " + newPattern);
                // TODO: cointains logic should be based on pattern code; pattern code should
                // based on types of vertices and edges
                Optional<Pattern> existingPattern = this.containsPattern(newPattern);
                if (!existingPattern.isPresent()) {
                    this.addPattern(newPattern);
                    this.addPatternEdge(pattern, newPattern);
                    System.out.println("add new pattern");
                    // System.out.println("add new pattern " + newPattern.toString());
                    // System.out.println("after add new pattern, glogue: " + this);
                    patternQueue.add(newPattern);
                } else {
                    if (!this.containsPatternEdge(pattern, existingPattern.get())) {
                        DefaultEdge defaultEdge = this.addPatternEdge(pattern, existingPattern.get());
                        System.out.println("pattern already exists, add edge " + defaultEdge);
                    } else {
                        System.out.println("pattern already exists, edge already exists");
                    }
                }
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        return this;
    }

    private Optional<Pattern> containsPattern(Pattern pattern) {
        for (Pattern p : this.gLogueGraph.vertexSet()) {
            if (p.equals(pattern)) {
                return Optional.of(p);
            } 
        }
        return Optional.empty();
    }

    private boolean containsPatternEdge(Pattern pattern1, Pattern pattern2) {
        return this.gLogueGraph.containsEdge(pattern1, pattern2);
    }

    private void addPattern(Pattern pattern) {
        this.gLogueGraph.addVertex(pattern);
    }

    private DefaultEdge addPatternEdge(Pattern pattern1, Pattern pattern2) {
       return gLogueGraph.addEdge(pattern1, pattern2);
    }

    @Override
    public String toString() {
        return "Vertices: " + this.gLogueGraph.vertexSet() + ", Edges: " + this.gLogueGraph.edgeSet();
    }

    public static void main(String[] args) {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
        System.out.println("NewGlogue " + gl.toString());
    }
}
