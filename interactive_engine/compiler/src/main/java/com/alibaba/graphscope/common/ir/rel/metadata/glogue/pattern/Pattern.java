package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.color.ColorRefinementAlgorithm;
import org.jgrapht.alg.interfaces.VertexColoringAlgorithm.Coloring;
import org.jgrapht.alg.isomorphism.ColorRefinementIsomorphismInspector;
import org.jgrapht.alg.isomorphism.IsomorphicGraphMapping;
import org.jgrapht.alg.isomorphism.IsomorphismInspector;
import org.jgrapht.alg.isomorphism.VF2GraphIsomorphismInspector;
import org.jgrapht.graph.AsUndirectedGraph;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.SimpleDirectedGraph;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.Combinations;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Pattern {
    /// pattern id, i.e., the index of the pattern in Glogue
    private int id;
    /// pattern graph, i.e., the topology of the pattern
    private final Graph<PatternVertex, PatternEdge> patternGraph;
    /// maxVertexId and maxEdgeId to record the max vertex id and max edge id in the
    /// pattern
    private int maxVertexId;
    private int maxEdgeId;
    // PatternOrder is used for reordering vertices in Pattern.
    // Noticed that it is not an identifier of Pattern. i.e., two patterns with same
    // pattern ordering may not be isomorphic.
    private PatternOrder patternOrder;

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

    // If isMultipleEdge is true, then use DirectedPseudograph. This represents a
    // fuzzy pattern.
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
        this.encoding();
    }

    public Pattern(Pattern pattern) {
        // TODO: pattern must be DirectedPseudograph or SimpleDirectedGraph?
        if (pattern.patternGraph.getType().isAllowingMultipleEdges()) {
            this.patternGraph = new DirectedPseudograph<PatternVertex, PatternEdge>(PatternEdge.class);
        } else {
            this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        }
        for (PatternVertex vertex : pattern.getVertexSet()) {
            addVertex(vertex);
        }
        for (PatternEdge edge : pattern.getEdgeSet()) {
            addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        }
        this.maxVertexId = pattern.maxVertexId;
        this.maxEdgeId = pattern.maxEdgeId;
        this.patternOrder = pattern.patternOrder;
    }

    public Pattern(PatternVertex vertex) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
        this.encoding();
    }

    public Pattern(Integer vertexTypeId) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        PatternVertex vertex = new PatternVertex(vertexTypeId);
        this.patternGraph.addVertex(vertex);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
        this.encoding();
    }

    public void setPatternId(int id) {
        this.id = id;
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

    /// Extend current pattern with the given extendStep, and return the new
    /// pattern.
    public Pattern extend(ExtendStep extendStep) {
        Pattern newPattern = new Pattern(this);
        Integer targetVertexTypeId = extendStep.getTargetVertexType();
        PatternVertex targetVertex = new PatternVertex(targetVertexTypeId, newPattern.maxVertexId);
        newPattern.addVertex(targetVertex);
        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
            PatternDirection dir = extendEdge.getDirection();
            Integer srcVertexOrder = extendEdge.getSrcVertexOrder();
            PatternVertex srcVertex = newPattern.getVertexByOrder(srcVertexOrder);
            EdgeTypeId edgeTypeId = extendEdge.getEdgeTypeId();
            // TODO: be very careful if we allow "both" direction in schema
            if (dir.equals(PatternDirection.OUT)) {
                System.out.println("To extend: " + srcVertex + " -> " + targetVertex + " " + edgeTypeId);
                PatternEdge edge = new PatternEdge(srcVertex, targetVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(srcVertex, targetVertex, edge);
            } else {
                System.out.println("To extend: " + targetVertex + " -> " + srcVertex + " " + edgeTypeId);
                PatternEdge edge = new PatternEdge(targetVertex, srcVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(targetVertex, srcVertex, edge);
            }

        }
        newPattern.encoding();
        return newPattern;
    }

    public void encoding() {
        PatternOrderCanonicalLabelingImpl patternOrder = new PatternOrderCanonicalLabelingImpl(this.patternGraph);
        this.patternOrder = patternOrder;
    }

    // add a pattern vertex into pattern, and increase pattern's maxVertexId
    public boolean addVertex(PatternVertex vertex) {
        boolean added = this.patternGraph.addVertex(vertex);
        if (added) {
            this.maxVertexId++;
        }
        return added;
    }

    public boolean addEdge(PatternVertex srcVertex, PatternVertex dstVertex, EdgeTypeId edgeTypeId) {
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
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    public PatternVertex getVertexByOrder(int vertexId) {
        return this.patternOrder.getVertexByOrder(vertexId);
    }

    public Integer getVertexOrder(PatternVertex vertex) {
        return this.patternOrder.getVertexOrder(vertex);
    }

    public Integer getVertexGroup(PatternVertex vertex) {
        return this.patternOrder.getVertexGroup(vertex);
    }

    // Find all possible ExtendSteps of current pattern based on the given Pattern
    // Meta
    public List<ExtendStep> getExtendSteps(GlogueSchema schema) {
        List<ExtendStep> extendSteps = new ArrayList<>();
        // Get all vertex labels from pattern meta as the possible extend target vertex
        List<Integer> targetVertexTypes = schema.getVertexTypes();
        // targetVertexTypeId -> List of ExtendEdges extend to targetVertex
        Map<Integer, List<ExtendEdge>> extendEdgesWithDstType = new HashMap<>();
        for (Integer targetVertexType : targetVertexTypes) {
            // for each targetVertexType, consider possible extend edges from each
            // srcPatternVertexType to targetVertexType
            // Notice that before consider extend edges, first check if the
            // srcPatternVertexType is already considered (i.e., if any
            // vertex from the same color group is already considered)
            Set<Integer> consideredGroup = new HashSet<>();
            for (PatternVertex srcPatternVertex : this.getVertexSet()) {
                Integer srcVertexOrder = this.getVertexOrder(srcPatternVertex);
                if (consideredGroup.contains(this.getVertexGroup(srcPatternVertex))) {
                    continue;
                } else {
                    consideredGroup.add(srcVertexOrder);
                }
                Integer srcVertexType = srcPatternVertex.getVertexTypeId();
                // Get all adjacent edges from srcVertex to targetVertex
                List<EdgeTypeId> outEdges = schema.getEdgeTypes(srcVertexType, targetVertexType);
                for (EdgeTypeId outEdge : outEdges) {
                    if (srcVertexType.equals(outEdge.getSrcLabelId())) {
                        ExtendEdge extendEdge = new ExtendEdge(
                                srcVertexOrder,
                                outEdge,
                                PatternDirection.OUT);
                        if (extendEdgesWithDstType.containsKey(outEdge.getDstLabelId())) {
                            extendEdgesWithDstType.get(outEdge.getDstLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstType.put(outEdge.getDstLabelId(),
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
                                srcVertexOrder,
                                inEdge,
                                PatternDirection.IN);
                        if (extendEdgesWithDstType.containsKey(inEdge.getSrcLabelId())) {
                            extendEdgesWithDstType.get(inEdge.getSrcLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstType.put(inEdge.getSrcLabelId(),
                                    new ArrayList<ExtendEdge>(Arrays.asList(extendEdge)));
                        }
                    } else {
                        System.out.println("very weird 222");
                    }
                }
            }
        }

        // get all subsets of extendEdgesWithDstId. Each subset corresponds to a
        // possible extend.
        for (Map.Entry entry : extendEdgesWithDstType.entrySet()) {
            List<ExtendEdge> orginalSet = (List<ExtendEdge>) entry.getValue();
            for (int k = 1; k <= orginalSet.size(); k++) {
                List<List<ExtendEdge>> subsets = Combinations.getCombinations(orginalSet, k);
                // TODO: a subset with duplicated edges, should be filter out?
                // ! e.g., do we need extend pattern like: person <-> person
                for (List<ExtendEdge> subset : subsets) {
                    extendSteps.add(new ExtendStep((Integer) entry.getKey(), subset));
                }
            }
        }

        return extendSteps;
    }

    @Override
    public String toString() {
        return "Pattern " + this.id + " Vertices: " + this.patternGraph.vertexSet().toString()
                + ", PatternEdges: "
                + this.patternGraph.edgeSet().toString() + ", PatternOrder: " + this.patternOrder;
    }

    // this should be based on the canonical labeling of pattern graph
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pattern) {
            Pattern other = (Pattern) obj;
            // compare vertex number and edge number
            if (this.maxVertexId != other.maxVertexId
                    || this.maxEdgeId != other.maxEdgeId) {
                return false;
            }
            // compare pattern order
            if (this.patternOrder == null) {
                this.encoding();
            }
            if (other.patternOrder == null) {
                other.encoding();
            }
            if (!this.patternOrder.equals(other.patternOrder)) {
                VF2GraphIsomorphismInspector isomorphismInspector = new VF2GraphIsomorphismInspector(this.patternGraph,
                        other.patternGraph, vertexComparator, edgeComparator);
                if (isomorphismInspector.isomorphismExists()) {
                    System.out.println("!!!Notice that different pattern order, but the same pattern!!!");
                    System.out.println("pattern1 v.s. pattern2: \n" + this + "\n" + other);
                    System.out.println(
                            "pattern order 1 v.s. pattern order 2 " + this.patternOrder + " " +
                                    other.patternOrder);
                    return true;
                } else {
                    return false;
                }
            } else {
                VF2GraphIsomorphismInspector isomorphismInspector = new VF2GraphIsomorphismInspector(this.patternGraph,
                        other.patternGraph, vertexComparator, edgeComparator);
                if (isomorphismInspector.isomorphismExists()) {
                    return true;
                } else {
                    System.out.println(
                            "It's okay: the same pattern order: " + this.patternOrder + " but not the same pattern");
                    System.out.println("pattern1 v.s. pattern2: \n" + this + "\n" + other);
                    System.out.println(
                            "pattern order 1 v.s. pattern order 2 " + this.patternOrder + " " +
                                    other.patternOrder);
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.patternGraph.hashCode();
    }

    public static void main(String[] args) {
        // v0: software
        // v1: person
        // v2: person
        PatternVertex v0 = new PatternVertex(1, 0);
        PatternVertex v1 = new PatternVertex(0, 1);
        PatternVertex v2 = new PatternVertex(0, 2);

        // person -> software
        EdgeTypeId e1 = new EdgeTypeId(0, 1, 1);
        // person -> person
        EdgeTypeId e2 = new EdgeTypeId(0, 0, 0);

        // p1 -> s0 <- p2
        Pattern p = new Pattern();
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v1, v0, e1);
        p.addEdge(v2, v0, e1);

        System.out.println("pattern p " + p);

        // p0 -> s1 <- p2
        Pattern p1 = new Pattern();

        PatternVertex v00 = new PatternVertex(0, 0);
        PatternVertex v11 = new PatternVertex(1, 1);
        PatternVertex v22 = new PatternVertex(0, 2);
        p1.addVertex(v00);
        p1.addVertex(v11);
        p1.addVertex(v22);
        p1.addEdge(v00, v11, e1);
        p1.addEdge(v22, v11, e1);
        System.out.println("pattern p1 " + p1);

        // p1 -> s0 -> p2
        Pattern p4 = new Pattern();
        p4.addVertex(v0);
        p4.addVertex(v1);
        p4.addVertex(v2);
        p4.addEdge(v1, v0, e1);
        // a fake edge
        EdgeTypeId e3 = new EdgeTypeId(1, 0, 2);
        p4.addEdge(v0, v2, e3);

        VF2GraphIsomorphismInspector isomorphismInspector = new VF2GraphIsomorphismInspector(p.patternGraph,
                p1.patternGraph);
        System.out.println("pattern isomorphic of p v.s. p1 " + isomorphismInspector.isomorphismExists());

        Graph<PatternVertex, PatternEdge> undirectedP = new AsUndirectedGraph<>(p.patternGraph);
        Graph<PatternVertex, PatternEdge> undirectedP1 = new AsUndirectedGraph<>(p1.patternGraph);

        // ColorRefinementIsomorphismInspector colorRefinementIsomorphismInspector = new
        // ColorRefinementIsomorphismInspector(
        // p.patternGraph, p1.patternGraph);

        ColorRefinementIsomorphismInspector colorRefinementIsomorphismInspector = new ColorRefinementIsomorphismInspector(
                undirectedP, undirectedP1);

        // TODO: throws exception when using color refinement to check isomorphism on
        // directed graph
        System.out.println("pattern isomorphic check of p v.s. p1 by color refinement "
                + colorRefinementIsomorphismInspector.isomorphismExists());

        ColorRefinementIsomorphismInspector colorRefinementIsomorphismInspector04 = new ColorRefinementIsomorphismInspector(
                p.patternGraph, p4.patternGraph);

        System.out.println("pattern isomorphic check of p v.s. p4 by color refinement "
                + colorRefinementIsomorphismInspector04.isomorphismExists());

        // p2 -> s0 <- p1 + p1 -> p2
        Pattern p2 = new Pattern(p);
        p2.addEdge(v1, v2, e2);
        System.out.println("pattern p2 " + p2);

        Pattern p3 = new Pattern(p);
        p3.addEdge(v2, v1, e2);
        System.out.println("pattern p3 " + p3);

        System.out.println("pattern 2 equals pattern 3 " + p2.equals(p3));

        VF2GraphIsomorphismInspector isomorphismInspector23 = new VF2GraphIsomorphismInspector(p2.patternGraph,
                p3.patternGraph);
        System.out.println("pattern isomorphic of p2 v.s. p3 " + isomorphismInspector23.isomorphismExists());

        ColorRefinementIsomorphismInspector colorRefinementIsomorphismInspector23 = new ColorRefinementIsomorphismInspector(
                p2.patternGraph, p3.patternGraph);

        System.out.println("pattern isomorphic check of p2 v.s. p3 by color refinement "
                + colorRefinementIsomorphismInspector23.isomorphismExists());

        System.out.println("mappings:");

        System.out.println("vf2 mappings of p v.s. p1:");
        Iterator<GraphMapping<PatternVertex, PatternEdge>> mappings = isomorphismInspector.getMappings();
        while (mappings.hasNext()) {
            GraphMapping<PatternVertex, PatternEdge> mapping = mappings.next();
            System.out.println("mapping " + mapping);
            for (PatternVertex vertex : p2.patternGraph.vertexSet()) {
                System.out.println(
                        "vertex " + vertex + " mapping " + mapping.getVertexCorrespondence(vertex, true) + ", "
                                + mapping.getVertexCorrespondence(vertex, false));

            }
        }

        // TODO: throws exception

        // System.out.println("color refinement mapping checking of p v.s. p1:");
        // Iterator<GraphMapping<PatternVertex, PatternEdge>> colorRefinementMappings =
        // colorRefinementIsomorphismInspector
        // .getMappings();
        // while (colorRefinementMappings.hasNext()) {
        // GraphMapping<PatternVertex, PatternEdge> mapping =
        // colorRefinementMappings.next();
        // System.out.println("mapping " + mapping);
        // for (PatternVertex vertex : p2.patternGraph.vertexSet()) {
        // System.out.println(
        // "vertex " + vertex + " mapping " + mapping.getVertexCorrespondence(vertex,
        // true) + ", "
        // + mapping.getVertexCorrespondence(vertex, false));

        // }
        // }

        System.out.println("vf2 mapping checking of p2 v.s. p3:");
        Iterator<GraphMapping<PatternVertex, PatternEdge>> mappings23 = isomorphismInspector23.getMappings();
        while (mappings23.hasNext()) {
            GraphMapping<PatternVertex, PatternEdge> mapping = mappings23.next();
            System.out.println("mapping " + mapping);
            for (PatternVertex vertex : p2.patternGraph.vertexSet()) {
                System.out.println(
                        "vertex " + vertex + " mapping " + mapping.getVertexCorrespondence(vertex, true) + ", "
                                + mapping.getVertexCorrespondence(vertex, false));

            }
        }

        System.out.println("color refinement mapping checking of p2 v.s. p3:");
        Iterator<GraphMapping<PatternVertex, PatternEdge>> colorRefinementMappings23 = colorRefinementIsomorphismInspector23
                .getMappings();
        while (colorRefinementMappings23.hasNext()) {
            GraphMapping<PatternVertex, PatternEdge> mapping = colorRefinementMappings23.next();
            System.out.println("mapping " + mapping);
            for (PatternVertex vertex : p2.patternGraph.vertexSet()) {
                System.out.println(
                        "vertex " + vertex + " mapping " + mapping.getVertexCorrespondence(vertex, true) + ", "
                                + mapping.getVertexCorrespondence(vertex, false));

            }
        }

        System.out.println("Graph colors: ...");
        ColorRefinementAlgorithm colorRefinementAlgorithm = new ColorRefinementAlgorithm(p.patternGraph);
        Coloring<PatternVertex> color = colorRefinementAlgorithm.getColoring();
        System.out.println("p color " + color);
        ColorRefinementAlgorithm colorRefinementAlgorithm1 = new ColorRefinementAlgorithm(p1.patternGraph);
        Coloring<PatternVertex> color1 = colorRefinementAlgorithm1.getColoring();
        System.out.println("p1 color " + color1);

        PatternOrderCanonicalLabelingImpl canonicalLabelManager = new PatternOrderCanonicalLabelingImpl(p.patternGraph);
        System.out.println("p0 canonical label " + canonicalLabelManager.toString());
        PatternOrderCanonicalLabelingImpl canonicalLabelManager1 = new PatternOrderCanonicalLabelingImpl(
                p1.patternGraph);
        System.out.println("p1 canonical label " + canonicalLabelManager1.toString());

        PatternOrderCanonicalLabelingImpl canonicalLabelManager2 = new PatternOrderCanonicalLabelingImpl(
                p2.patternGraph);
        System.out.println("p2 canonical label " + canonicalLabelManager2.toString());
        PatternOrderCanonicalLabelingImpl canonicalLabelManager3 = new PatternOrderCanonicalLabelingImpl(
                p3.patternGraph);
        System.out.println("p3 canonical label " + canonicalLabelManager3.toString());

        Pattern p5 = new Pattern();
        p5.addVertex(v0);
        p5.addVertex(v1);
        PatternVertex v222 = new PatternVertex(0, 2);
        p5.addVertex(v222);
        p5.addEdge(v1, v0, e1);
        p5.addEdge(v1, v222, e2);

        Pattern p6 = new Pattern();
        p6.addVertex(v0);
        p6.addVertex(v1);
        PatternVertex v2222 = new PatternVertex(1, 2);
        p6.addVertex(v2222);
        p6.addEdge(v1, v0, e1);
        p6.addEdge(v1, v2222, e1);

        System.out.println("p5 " + p5);
        System.out.println("p6 " + p6);

        Graph<PatternVertex, PatternEdge> undirectedP5 = new AsUndirectedGraph<>(p5.patternGraph);
        Graph<PatternVertex, PatternEdge> undirectedP6 = new AsUndirectedGraph<>(p6.patternGraph);

        ColorRefinementIsomorphismInspector colorRefinementIsomorphismInspector56 = new ColorRefinementIsomorphismInspector(
                undirectedP5, undirectedP6);

        System.out.println("pattern isomorphic check of p5 v.s. p6 by color refinement "
                + colorRefinementIsomorphismInspector56.isomorphismExists());

        // software
        PatternVertex v70 = new PatternVertex(1, 0);
        // person
        PatternVertex v71 = new PatternVertex(0, 1);
        // software <- person
        Pattern p7 = new Pattern();
        p7.addVertex(v70);
        p7.addVertex(v71);
        p7.addEdge(v71, v70, e1);

        // person
        PatternVertex v80 = new PatternVertex(0, 0);
        // software
        PatternVertex v81 = new PatternVertex(1, 1);
        // person -> software
        Pattern p8 = new Pattern();
        p8.addVertex(v80);
        p8.addVertex(v81);
        p8.addEdge(v80, v81, e1);

        System.out.println("p7 " + p7);
        System.out.println("p8 " + p8);
        PatternOrderCanonicalLabelingImpl canonicalLabelManager7 = new PatternOrderCanonicalLabelingImpl(
                p7.patternGraph);
        System.out.println("p7 canonical label " + canonicalLabelManager7.toString());
        PatternOrderCanonicalLabelingImpl canonicalLabelManager8 = new PatternOrderCanonicalLabelingImpl(
                p8.patternGraph);
        System.out.println("p8 canonical label " + canonicalLabelManager8.toString());

    }
}
