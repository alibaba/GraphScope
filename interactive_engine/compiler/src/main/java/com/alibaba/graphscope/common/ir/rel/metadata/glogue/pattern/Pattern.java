package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.alg.color.ColorRefinementAlgorithm;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.Coloring;
import org.jgrapht.alg.isomorphism.ColorRefinementIsomorphismInspector;
import org.jgrapht.alg.isomorphism.IsomorphicGraphMapping;
import org.jgrapht.alg.isomorphism.IsomorphismInspector;
import org.jgrapht.alg.isomorphism.VF2GraphIsomorphismInspector;
import org.jgrapht.alg.isomorphism.VF2SubgraphIsomorphismInspector;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.SimpleDirectedGraph;

import com.alibaba.graphscope.common.ir.rel.graph.pattern.PatternCode;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.Combinations;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Pattern {
    private final Graph<PatternVertex, PatternEdge> patternGraph;
    private int maxVertexId;
    private int maxEdgeId;
    // pattern position in Glogue
    private Integer position;
    
    // vertex comparator and edge comparator are used for isomorphism inspector
    final static Comparator<PatternVertex> vertexComparator = (o1, o2) -> o1.getVertexTypeId()
            .compareTo(o2.getVertexTypeId());
    final static Comparator<PatternEdge> edgeComparator = (o1, o2) -> o1.getEdgeTypeId().compareTo(o2.getEdgeTypeId());

    // by default, simple directed graph is used for pattern representation.
    public Pattern() {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.maxVertexId = 0;
        this.maxEdgeId = 0;
    }

    // If isMultipleEdge is true, then use DirectedPseudograph. This represents a fuzzy pattern.
    public Pattern(boolean isMultipleEdge) {
        if (isMultipleEdge) {
            this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        } else {
            this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        }
        this.maxVertexId = 0;
        this.maxEdgeId = 0;
    }

    public Pattern(Graph<PatternVertex, PatternEdge> patternGraph) {
        this.patternGraph = patternGraph;
        this.maxVertexId = patternGraph.vertexSet().size();
        this.maxEdgeId = patternGraph.edgeSet().size();
    }

    public Pattern(Pattern pattern) {
        if (pattern.patternGraph.getType().isAllowingMultipleEdges()) {
           this.patternGraph =  new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        } else {
           this.patternGraph =  new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        }
        for (var vertex : pattern.getVertexSet()) {
            addVertex(vertex);
        }
        for (var edge : pattern.getEdgeSet()) {
            addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        }
        this.maxVertexId = pattern.maxVertexId;
        this.maxEdgeId = pattern.maxEdgeId;
    }

    public Pattern(PatternVertex vertex) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
    }

    public Pattern(Integer vertexTypeId) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        PatternVertex vertex = new PatternVertex(vertexTypeId);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
    }

    public int size() {
        return this.patternGraph.vertexSet().size();
    }

    public Set<PatternVertex> getVertexSet() {
        return this.patternGraph.vertexSet();
    }

    public Set<PatternEdge> getEdgeSet() {
        return this.patternGraph.edgeSet();
    }

    public Graph<PatternVertex, PatternEdge> getPatternGraph() {
        return this.patternGraph;
    }

    public Pattern extend(ExtendStep extendStep) {
        Pattern newPattern = new Pattern(this);
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
        return newPattern;
    }

    public PatternCode encoding() {
        return new PatternCode(this);
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
        PatternVertex vertex = new PatternVertex(vertexTypeId, this.maxVertexId);
        return addVertex(vertex);
    }

    private boolean addEdge(PatternVertex srcVertex, PatternVertex dstVertex, EdgeTypeId edgeTypeId) {
        PatternEdge edge = new PatternEdge(srcVertex, dstVertex, edgeTypeId, this.maxEdgeId);
        return addEdge(srcVertex, dstVertex, edge);
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
            if (vertex.getPosition().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    public PatternVertex getVertexByRank(int vertexRank) {
        // TODO: more efficient way to find vertex by rank
        for (PatternVertex vertex : this.patternGraph.vertexSet()) {
            if (vertex.getId() == vertexRank) {
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
            for (PatternVertex srcPatternVertex : this.getVertexSet()) {
                Integer srcVertexType = srcPatternVertex.getVertexTypeId();
                // Get all adjacent edges from srcVertex to targetVertex
                List<EdgeTypeId> outEdges = schema.getEdgeTypes(srcVertexType, targetVertexType);
                for (EdgeTypeId outEdge : outEdges) {
                    if (srcVertexType.equals(outEdge.getSrcLabelId())) {
                        ExtendEdge extendEdge = new ExtendEdge(
                                srcPatternVertex.getId(),
                                outEdge,
                                PatternDirection.OUT);
                        if (extendEdgesWithDstId.containsKey(outEdge.getDstLabelId())) {
                            extendEdgesWithDstId.get(outEdge.getDstLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstId.put(outEdge.getDstLabelId(),
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
                List<EdgeTypeId> inEdges = schema.getEdgeTypes(targetVertexType, srcVertexType);
                for (EdgeTypeId inEdge : inEdges) {
                    if (srcVertexType.equals(inEdge.getDstLabelId())) {
                        ExtendEdge extendEdge = new ExtendEdge(
                                srcPatternVertex.getId(),
                                inEdge,
                                PatternDirection.IN);
                        if (extendEdgesWithDstId.containsKey(inEdge.getSrcLabelId())) {
                            extendEdgesWithDstId.get(inEdge.getSrcLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstId.put(inEdge.getSrcLabelId(),
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
                // TODO: a subset with duplicated edges, should be filter out?! e.g., do not extend pattern like: person <-> person
                for (List<ExtendEdge> subset : subsets) {
                    extendSteps.add(new ExtendStep((Integer) entry.getKey(), subset));
                }
            }
        }

        return extendSteps;
    }

    @Override
    public String toString() {
        return this.patternGraph.vertexSet().toString() + this.patternGraph.edgeSet().toString();
    }

    // this should be based on the canonical labeling of pattern graph
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pattern) {
            Pattern other = (Pattern) obj;
            if (this.patternGraph.vertexSet().size() != other.patternGraph.vertexSet().size()
                    || this.patternGraph.edgeSet().size() != other.patternGraph.edgeSet().size()) {
                return false;
            } 
            // TODO: more filtering rather than directly using isomorphism inspector
            else if (!this.patternGraph.getType().isAllowingMultipleEdges()
                    && !other.patternGraph.getType().isAllowingMultipleEdges()) {
                VF2GraphIsomorphismInspector isomorphismInspector = new VF2GraphIsomorphismInspector(this.patternGraph,
                        other.patternGraph, vertexComparator, edgeComparator);
                return isomorphismInspector.isomorphismExists();
            } else {
                // TODO: if multiple edges are allowed, we need to process this case.
                return this.patternGraph.equals(other.patternGraph);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.patternGraph.hashCode();
    }

    public static void main(String[] args) {
        EdgeTypeId e = new EdgeTypeId(11, 22, 1122);

        Pattern p = new Pattern();
        PatternVertex v0 = new PatternVertex(11, 0);
        PatternVertex v1 = new PatternVertex(22, 1);
        PatternVertex v2 = new PatternVertex(11, 0);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v0, v1, e);
        p.addEdge(v2, v1, e);

        System.out.println("pattern " + p);

        List<ExtendStep> extendSteps = p.getExtendSteps(new GlogueSchema().DefaultGraphSchema());
        System.out.println("extend steps: ");
        extendSteps.forEach(System.out::println);

        for (ExtendStep extendStep : extendSteps) {
            System.out.println(extendStep);
            Pattern newPattern = p.extend(extendStep);
            System.out.println("new pattern " + newPattern);
        }

        Pattern p1 = new Pattern(false);

        p1.addVertex(v0);
        p1.addVertex(v1);
        p1.addVertex(v2);
        p1.addEdge(v2, v1, e);
        p1.addEdge(v0, v1, e);
        System.out.println("pattern p1 " + p1);

        System.out.println("pattern equals pattern 1 " + p.equals(p1));

        VF2GraphIsomorphismInspector isomorphismInspector = new VF2GraphIsomorphismInspector(p.patternGraph,
                p1.patternGraph);
        System.out.println("pattern isomorphic to pattern 1 " + isomorphismInspector.isomorphismExists());

        Pattern p2 = new Pattern(false);
        PatternVertex v00 = new PatternVertex(22, 0);
        PatternVertex v11 = new PatternVertex(11, 1);
        p2.addVertex(v00);
        p2.addVertex(v11);
        p2.addEdge(v11, v00, e);

        System.out.println("pattern p2 " + p2);

        Pattern p3 = new Pattern(false);
        PatternVertex v000 = new PatternVertex(11, 0);
        PatternVertex v111 = new PatternVertex(22, 1);
        p3.addVertex(v000);
        p3.addVertex(v111);
        p3.addEdge(v000, v111, e);

        System.out.println("pattern p3 " + p3);

        System.out.println("pattern 2 equals pattern 3 " + p2.equals(p3));

        Comparator<PatternVertex> vertexComparator = (o1, o2) -> o1.getVertexTypeId().compareTo(o2.getVertexTypeId());
        Comparator<PatternEdge> edgeComparator = (o1, o2) -> o1.getEdgeTypeId().compareTo(o2.getEdgeTypeId());
        VF2GraphIsomorphismInspector isomorphismInspector2 = new VF2GraphIsomorphismInspector(p2.patternGraph,
                p3.patternGraph, vertexComparator, edgeComparator);
        System.out.println("pattern isomorphic to pattern 1 " + isomorphismInspector2.isomorphismExists());

        ColorRefinementAlgorithm colorRefinementAlgorithm = new ColorRefinementAlgorithm(p.patternGraph);
        Coloring<PatternVertex> color = colorRefinementAlgorithm.getColoring();
        System.out.println("color " + color);
    }
}
