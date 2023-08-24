package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternOrdering;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Glogue {
    // the topology of GLogue graph
    private Graph<Pattern, GlogueEdge> glogueGraph;
    // the index for pattern count query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern count
    // private HashMap<PatternCode, Double> patternCardinalityMap;
    private GlogueCardinalityEstimation glogueCardinalityEstimation;
    // the index for pattern locate query
    // key: pattern code of PatternGraph, i.e., vertices in gLogueGraph
    // value: pattern locate
    private HashMap<PatternOrdering, Integer> patternPositionMap;
    private List<Pattern> roots;

    protected Glogue() {
        this.glogueGraph = new DirectedPseudograph<Pattern, GlogueEdge>(GlogueEdge.class);
        this.patternPositionMap = new HashMap<PatternOrdering, Integer>();
        this.roots = new ArrayList<>();
    }

    // Construct NewGlogue from a graph schema with default max pattern size = 3
    public Glogue create(GlogueSchema schema) {
        return this.create(schema, 3);
    }

    // Construct NewGlogue from a graph schema with given max pattern size
    public Glogue create(GlogueSchema schema, int maxPatternSize) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        for (Integer vertexTypeId : schema.getVertexTypes()) {
            PatternVertex vertex = new PatternVertex(vertexTypeId);
            Pattern new_pattern = new Pattern(vertex);
            this.addPattern(new_pattern);
            this.addRoot(new_pattern);
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
                Optional<Pattern> existingPattern = this.containsPattern(newPattern);
                if (!existingPattern.isPresent()) {
                    System.out.println("add new pattern");
                    this.addPattern(newPattern);
                    Map<Integer, Integer> srcToDstIdMapping = this.computeIdMapping(pattern, newPattern);
                    this.addPatternEdge(pattern, newPattern, extendStep, srcToDstIdMapping);
                    patternQueue.add(newPattern);
                } else {
                    if (!this.containsPatternEdge(pattern, existingPattern.get())) {
                        System.out.println(
                                "pattern already exists: " + existingPattern.get());
                        // notice that the IdMapping should be computed based on pattern and newPattern,
                        // not pattern and existingPattern
                        Map<Integer, Integer> srcToDstIdMapping = this.computeIdMapping(pattern, newPattern);
                        this.addPatternEdge(pattern, existingPattern.get(), extendStep, srcToDstIdMapping);
                    } else {
                        System.out
                                .println("pattern already exists: " + existingPattern.get() + ", edge already exists");
                    }
                }
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        System.out.println("NewGlogue " + this.toString());
        System.out.println();
        System.out.println();
        // compute pattern cardinality
        this.glogueCardinalityEstimation = new GlogueBasicCardinalityEstimationImpl().create(this, schema);

        System.out.println("GlogueBasicCardinalityEstimationImpl " + this.glogueCardinalityEstimation.toString());

        return this;
    }

    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        return glogueGraph.outgoingEdgesOf(pattern);

    }

    public Set<GlogueEdge> getInEdge(Pattern pattern) {
        return glogueGraph.incomingEdgesOf(pattern);
    }

    private void addRoot(Pattern pattern) {
        this.roots.add(pattern);
    }

    public List<Pattern> getRoots() {
        return roots;
    }

    private Optional<Pattern> containsPattern(Pattern pattern) {
        for (Pattern p : this.glogueGraph.vertexSet()) {
            if (p.equals(pattern)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    private boolean containsPatternEdge(Pattern srcPattern, Pattern dstPattern) {
        return this.glogueGraph.containsEdge(srcPattern, dstPattern);
    }

    private boolean addPattern(Pattern pattern) {
        return this.glogueGraph.addVertex(pattern);
    }

    private boolean addPatternEdge(Pattern srcPattern, Pattern dstPattern, ExtendStep edge,
            Map<Integer, Integer> srcToDstIdMapping) {
        GlogueExtendIntersectEdge glogueEdge = new GlogueExtendIntersectEdge(srcPattern, dstPattern, edge,
                srcToDstIdMapping);
        System.out.println("add glogue edge " + glogueEdge);
        return this.glogueGraph.addEdge(srcPattern, dstPattern, glogueEdge);
    }

    // compute id mapping from src pattern to dst pattern. Notice that dst pattern must be extended from src pattern.
    private Map<Integer, Integer> computeIdMapping(Pattern srcPattern, Pattern dstPattern) {
        Map<Integer, Integer> srcToDstIdMapping = new HashMap<>();
        for (PatternVertex srcVertex : srcPattern.getVertexSet()) {
            Integer srcVertexId = srcPattern.getVertexId(srcVertex);
            // dstPattern is extended from srcPatter, so they have the same vertex position.
            PatternVertex dstVertex = dstPattern.getVertexByPosition(srcVertex.getPosition());
            Integer dstVertexId = dstPattern.getVertexId(dstVertex);
            srcToDstIdMapping.put(srcVertexId, dstVertexId);
        }
        return srcToDstIdMapping;
    }

    @Override
    public String toString() {
        return "Vertices: " + this.glogueGraph.vertexSet() + ", Edges: " + this.glogueGraph.edgeSet() + ", Roots: "
                + this.roots;
    }

    public static void main(String[] args) {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
    }
}
