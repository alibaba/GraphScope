package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.rmi.server.ExportException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kerby.util.SysUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.Multigraph;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.Combinations;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Pattern {
    private final Graph<PatternVertex, PatternEdge> patternGraph;
    private int maxVertexId;
    private int maxEdgeId;

    public Pattern() {
        this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.maxVertexId = 0;
        this.maxEdgeId = 0;
    }

    public Pattern(Graph<PatternVertex, PatternEdge> patternGraph) {
        this.patternGraph = patternGraph;
        this.maxVertexId = patternGraph.vertexSet().size();
        this.maxEdgeId = patternGraph.edgeSet().size();
    }

    public Pattern(Pattern pattern) {
        this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);

        for (var vertex : pattern.getVertexList()) {
            this.patternGraph.addVertex(vertex);
        }
        for (var edge : pattern.getPatternGraph().edgeSet()) {
            this.patternGraph.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        }
        this.maxVertexId = pattern.maxVertexId;
        this.maxEdgeId = pattern.maxEdgeId;
    }

    public Pattern(PatternVertex vertex) {
        this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
    }

    public Pattern(Integer vertexTypeId) {
        this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        PatternVertex vertex = new PatternVertex(vertexTypeId);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
    }

    public int size() {
        return this.patternGraph.vertexSet().size();
    }

    public List<PatternVertex> getVertexList() {
        return List.copyOf(this.patternGraph.vertexSet());
    }

    public List<PatternEdge> getEdgeList() {
        return List.copyOf(this.patternGraph.edgeSet());
    }

    public Graph<PatternVertex, PatternEdge> getPatternGraph() {
        return this.patternGraph;
    }

    public Pattern extend(ExtendStep extendStep) {
        Pattern newPattern = new Pattern(this);
        System.out.println("before extend " + newPattern);
        Integer targetVertexTypeId = extendStep.getTargetVertexType();
        PatternVertex targetVertex = new PatternVertex(targetVertexTypeId, newPattern.maxVertexId);
        newPattern.addVertex(targetVertex);
        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
            PatternDirection dir = extendEdge.getDirection();
            Integer srcVertexRank = extendEdge.getSrcVertexRank();
            PatternVertex srcVertex = newPattern.getVertexByRank(srcVertexRank);
            EdgeTypeId edgeTypeId = extendEdge.getEdgeTypeId();
            // TODO: be very careful if we allow "both" direction in schema
            if (dir.equals(PatternDirection.OUT)) {
                System.out.println("To extend: " + srcVertex + " -> " + targetVertex + " " + edgeTypeId);
                PatternEdge edge = new PatternEdge(srcVertex, targetVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(srcVertex, targetVertex, edge);
            } else {
                System.out.println("To extend: " + srcVertex + " <- " + targetVertex + " " + edgeTypeId);
                PatternEdge edge = new PatternEdge(targetVertex, srcVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(targetVertex, srcVertex, edge);
            }

        }
        // System.out.println("after extend " +
        // newPattern.patternGraph.edgesOf(targetVertex));
        return newPattern;
    }

    private boolean containsVertex(PatternVertex vertex) {
        return this.patternGraph.containsVertex(vertex);
    }

    // add a pattern vertex into pattern, and increase pattern's maxVertexId
    private boolean addVertex(PatternVertex vertex) {
        boolean added = this.patternGraph.addVertex(vertex);
        if (added) {
            this.maxVertexId++;
        }
        return added;
    }

    private boolean addVertex(Integer vertexTypeId) {
        PatternVertex vertex = new PatternVertex(vertexTypeId, this.maxVertexId++);
        return this.patternGraph.addVertex(vertex);
    }

    private boolean addEdge(PatternVertex srcVertex, PatternVertex dstVertex, EdgeTypeId edgeTypeId) {
        PatternEdge edge = new PatternEdge(srcVertex, dstVertex, edgeTypeId, this.maxEdgeId++);
        return this.patternGraph.addEdge(srcVertex, dstVertex, edge);
    }

    // add a pattern edge into pattern, and increase pattern's maxEdgeId
    private boolean addEdge(PatternVertex srcVertex, PatternVertex dstVertex, PatternEdge edge) {
        boolean added = this.patternGraph.addEdge(srcVertex, dstVertex, edge);
        if (added) {
            this.maxEdgeId++;
        }
        return added;
    }

    public PatternVertex getVertexById(Integer vertexId) {
        // TODO: more efficient way to find vertex by id
        for (PatternVertex vertex : this.patternGraph.vertexSet()) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    public PatternVertex getVertexByRank(int vertexRank) {
        // TODO: more efficient way to find vertex by rank
        for (PatternVertex vertex : this.patternGraph.vertexSet()) {
            if (vertex.getRank() == vertexRank) {
                return vertex;
            }
        }
        return null;
    }

    // Find all possible ExtendSteps of current pattern based on the given Pattern
    // Meta
    public List<ExtendStep> getExtendSteps(GlogueSchema schema) {
        List<ExtendStep> extendSteps = new ArrayList<>();
        // Get all vertex labels from pattern meta as the possible extend target vertex
        List<Integer> targetVertexTypes = schema.getVertexTypes();
        // targetVertexId -> List of ExtendEdges extend to targetVertex
        Map<Integer, List<ExtendEdge>> extendEdgesWithDstId = new HashMap<>();

        for (Integer targetVertexType : targetVertexTypes) {
            for (PatternVertex srcPatternVertex : this.getVertexList()) {
                Integer srcVertex = srcPatternVertex.getVertexTypeId();
                // Get all adjacent edges from srcVertex to targetVertex
                List<EdgeTypeId> outEdges = schema.getEdgeTypes(srcVertex, targetVertexType);
                for (EdgeTypeId adjacentEdge : outEdges) {
                    if (srcVertex.equals(adjacentEdge.getSrcLabelId())) {
                        ExtendEdge extendEdge = new ExtendEdge(
                                srcPatternVertex.getRank(),
                                adjacentEdge,
                                PatternDirection.OUT);
                        if (extendEdgesWithDstId.containsKey(adjacentEdge.getDstLabelId())) {
                            extendEdgesWithDstId.get(adjacentEdge.getDstLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstId.put(adjacentEdge.getDstLabelId(),
                                    new ArrayList<ExtendEdge>(Arrays.asList(extendEdge)));
                        }

                    } else {
                        System.out.println("very weird 111");
                    }
                }
                // Get all adjacent edges from targetVertex to srcVertex
                // TODO: be very careful here: if we allow "both" direction in schema, e.g.,
                // person-knows-person, then we need to consider the duplications in outEdges
                // and inEdges; that is, when extend a new person, then only one edge expanded.
                List<EdgeTypeId> inEdges = schema.getEdgeTypes(targetVertexType, srcVertex);
                for (EdgeTypeId adjacentEdge : inEdges) {
                    if (srcVertex.equals(adjacentEdge.getDstLabelId())) {
                        ExtendEdge extendEdge = new ExtendEdge(
                                srcPatternVertex.getRank(),
                                adjacentEdge,
                                PatternDirection.IN);
                        if (extendEdgesWithDstId.containsKey(adjacentEdge.getSrcLabelId())) {
                            extendEdgesWithDstId.get(adjacentEdge.getSrcLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstId.put(adjacentEdge.getSrcLabelId(),
                                    new ArrayList<ExtendEdge>(Arrays.asList(extendEdge)));
                        }
                    } else {
                        System.out.println("very weird 222");
                    }
                }
            }
        }

        /// get all subsets of extendEdgesWithDstId. Each subset corresponds to a
        /// possible extend.
        for (Map.Entry entry : extendEdgesWithDstId.entrySet()) {
            List<ExtendEdge> orginalSet = (List<ExtendEdge>) entry.getValue();
            for (int k = 1; k <= orginalSet.size(); k++) {
                List<List<ExtendEdge>> subsets = Combinations.getCombinations(orginalSet, k);
                for (List<ExtendEdge> subset : subsets) {
                    extendSteps.add(new ExtendStep((Integer) entry.getKey(), subset));
                }
            }
        }

        return extendSteps;
    }

    // // should be a util function
    // private Vec<> getSubsets

    @Override
    public String toString() {
        return this.patternGraph.vertexSet().toString() + this.patternGraph.edgeSet().toString() + ", max id"
                + this.maxVertexId + ", " + this.maxEdgeId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pattern) {
            Pattern other = (Pattern) obj;
            return this.patternGraph.equals(other.patternGraph);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.patternGraph.hashCode();
    }

    public static void main(String[] args) throws URISyntaxException, ExportException {
        EdgeTypeId e = new EdgeTypeId(11, 22, 1122);

        Pattern p = new Pattern();
        PatternVertex v0 = new PatternVertex(11, 0);
        PatternVertex v1 = new PatternVertex(22, 1);
        PatternVertex v2 = new PatternVertex(11, 2);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v0, v1, e);
        p.addEdge(v2, v1, e);

        System.out.println("pattern " + p);

        List<ExtendStep> extendSteps = p.getExtendSteps(new GlogueSchema().DefaultGraphSchema());
        // extend steps [ExtendStep{targetVertexType=22, extendEdges=[11->22, 11->22]},
        // ExtendStep{targetVertexType=11, extendEdges=[11->11, 11->11, 11->22,
        // 11->11,11->11]}]
        // Notice when targetVertexType is 11, there are 5 edges, but only 3 unique
        // edges (direction is considered; )
        // TODO: dedup via canonical labelling
        System.out.println("extend steps: ");
        extendSteps.forEach(System.out::println);

        for (ExtendStep extendStep : extendSteps) {
            System.out.println(extendStep);
            Pattern newPattern = p.extend(extendStep);
            System.out.println("new pattern " + newPattern);
        }
    }
}
