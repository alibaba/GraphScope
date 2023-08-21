package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;

import com.alibaba.graphscope.common.ir.rel.graph.pattern.PatternCode;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Glogue {
    // the topology of GLogue graph
    private Graph<Pattern, GlogueEdge> gLogueGraph;
    // the index for pattern count query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern count
    private HashMap<PatternCode, Double> patternCardinalityMap;
    // the index for pattern locate query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern locate
    private HashMap<PatternCode, Integer> patternPositionMap;
    private List<PatternCode> roots;

    protected Glogue() {
        this.gLogueGraph = new DirectedPseudograph<Pattern, GlogueEdge>(GlogueEdge.class);
        this.patternCardinalityMap = new HashMap<PatternCode, Double>();
        this.patternPositionMap = new HashMap<PatternCode, Integer>();
        this.roots = new ArrayList<>();
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
            this.addRootPattern(new_pattern);
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
                    this.addPatternEdge(pattern, newPattern, extendStep);
                    System.out.println("add new pattern");
                    // System.out.println("add new pattern " + newPattern.toString());
                    // System.out.println("after add new pattern, glogue: " + this);
                    patternQueue.add(newPattern);
                } else {
                    if (!this.containsPatternEdge(pattern, existingPattern.get())) {
                        this.addPatternEdge(pattern, existingPattern.get(), extendStep);
                        System.out.println(
                                "pattern already exists: " + existingPattern.get() + ", add edge " + extendStep);
                    } else {
                        System.out
                                .println("pattern already exists: " + existingPattern.get() + ", edge already exists");
                    }
                }
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        return this;
    }

    public Pattern getPatternByCode(PatternCode code) {
        // TODO: refine this; should get position by code first, and then get pattern
        // from gLogueGraph;
        return code.getPattern();
    }

    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        return gLogueGraph.outgoingEdgesOf(pattern);

    }

    public Set<GlogueEdge> getInEdge(Pattern pattern) {
        return gLogueGraph.incomingEdgesOf(pattern);
    }

    public List<PatternCode> getRoots() {
        return roots;
    }

    private void addRootPattern(Pattern pattern) {
        this.roots.add(pattern.encoding());
    }

    private Optional<Pattern> containsPattern(Pattern pattern) {
        for (Pattern p : this.gLogueGraph.vertexSet()) {
            if (p.equals(pattern)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    private boolean containsPatternEdge(Pattern srcPattern, Pattern dstPattern) {
        return this.gLogueGraph.containsEdge(srcPattern, dstPattern);
    }

    private boolean addPattern(Pattern pattern) {
        return this.gLogueGraph.addVertex(pattern);
    }

    private boolean addPatternEdge(Pattern srcPattern, Pattern dstPattern, GlogueEdge edge) {
        return this.gLogueGraph.addEdge(srcPattern, dstPattern, edge);
    }

    private boolean addPatternEdge(Pattern srcPattern, Pattern dstPattern, ExtendStep edge) {
        GlogueExtendIntersectEdge glogueEdge = new GlogueExtendIntersectEdge(srcPattern, dstPattern, edge);
        return addPatternEdge(srcPattern, dstPattern, glogueEdge);
    }

    @Override
    public String toString() {
        return "Vertices: " + this.gLogueGraph.vertexSet() + ", Edges: " + this.gLogueGraph.edgeSet() + ", Roots: "
                + this.roots;
    }

    public static void main(String[] args) {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
         System.out.println("NewGlogue " + gl.toString());
         System.out.println();
         System.out.println();
        GlogueBasicCardinalityEstimationImpl gce = new GlogueBasicCardinalityEstimationImpl().create(gl, g);
        System.out.println("GlogueBasicCardinalityEstimationImpl " + gce.toString());
       
    }
}
