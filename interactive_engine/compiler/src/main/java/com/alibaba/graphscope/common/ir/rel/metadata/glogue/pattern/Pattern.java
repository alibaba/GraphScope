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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.Combinations;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.alg.isomorphism.VF2GraphIsomorphismInspector;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    private final ConnectivityInspector<PatternVertex, PatternEdge> connectivityInspector;

    private static Logger logger = LoggerFactory.getLogger(Pattern.class);

    // vertex type comparator and edge type comparator are used for isomorphism
    // inspector
    static final Comparator<PatternVertex> vertexTypeComparator =
            Comparator.comparing(o -> o.getIsomorphismChecker());
    static final Comparator<PatternEdge> edgeTypeComparator =
            Comparator.comparing(o -> o.getIsomorphismChecker());

    public Pattern() {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.connectivityInspector = new ConnectivityInspector<>(this.patternGraph);
        this.maxVertexId = 0;
        this.maxEdgeId = 0;
    }

    public Pattern(Graph<PatternVertex, PatternEdge> patternGraph) {
        this.patternGraph = patternGraph;
        this.connectivityInspector = new ConnectivityInspector<>(this.patternGraph);
        this.maxVertexId = patternGraph.vertexSet().size();
        this.maxEdgeId = patternGraph.edgeSet().size();
        this.reordering();
    }

    public Pattern(Pattern pattern) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        for (PatternVertex vertex : pattern.getVertexSet()) {
            addVertex(vertex);
        }
        for (PatternEdge edge : pattern.getEdgeSet()) {
            addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        }
        this.connectivityInspector = new ConnectivityInspector<>(this.patternGraph);
        this.maxVertexId = pattern.maxVertexId;
        this.maxEdgeId = pattern.maxEdgeId;
        this.patternOrder = pattern.patternOrder;
    }

    public Pattern(PatternVertex vertex) {
        this.patternGraph = new SimpleDirectedGraph<PatternVertex, PatternEdge>(PatternEdge.class);
        this.patternGraph.addVertex(vertex);
        this.connectivityInspector = new ConnectivityInspector<>(this.patternGraph);
        this.maxVertexId = 1;
        this.maxEdgeId = 0;
        this.reordering();
    }

    public void setPatternId(int id) {
        this.id = id;
    }

    public Integer getVertexNumber() {
        return this.maxVertexId;
    }

    public Integer getEdgeNumber() {
        return this.maxEdgeId;
    }

    public Set<PatternVertex> getVertexSet() {
        return this.patternGraph.vertexSet();
    }

    public Set<PatternEdge> getEdgeSet() {
        return this.patternGraph.edgeSet();
    }

    public Set<PatternEdge> getEdgesOf(PatternVertex vertex) {
        return this.patternGraph.edgesOf(vertex);
    }

    /**
     * Get all possible extend steps of current pattern based on the given GlogueSchema
     * @param glogue schema
     * @return a list of extend steps
     */
    public List<ExtendStep> getExtendSteps(GlogueSchema schema) {
        // For each vertexType in GlogueSchema (i.e., targetPatternVertexType),
        // consider all possible extend steps from each vertex in current pattern (i.e.,
        // srcPatternVertexType) to targetPatternVertexType.
        List<ExtendStep> extendSteps = new ArrayList<>();
        // Get all vertex labels from pattern meta as the possible extend target vertex
        List<Integer> targetVertexTypes = schema.getVertexTypes();
        // targetVertexTypeId -> List of ExtendEdges extend to targetVertex
        Map<Integer, List<ExtendEdge>> extendEdgesWithDstType = new HashMap<>();
        for (Integer targetVertexType : targetVertexTypes) {
            Set<Integer> consideredGroup = new HashSet<>();
            for (PatternVertex srcPatternVertex : this.getVertexSet()) {
                Integer srcVertexOrder = this.getVertexOrder(srcPatternVertex);
                if (consideredGroup.contains(this.getVertexGroup(srcPatternVertex))) {
                    // Notice that before consider extend edges, skip if the
                    // srcPatternVertexType is already considered (i.e., if any
                    // vertex from the same color group is already considered)
                    continue;
                } else {
                    consideredGroup.add(srcVertexOrder);
                }
                if (srcPatternVertex.getVertexTypeIds().size() != 1) {
                    throw new UnsupportedOperationException(
                            "In ExtendStep, srcPatternVertex "
                                    + srcPatternVertex
                                    + " is of basic type");
                }
                Integer srcVertexType = srcPatternVertex.getVertexTypeIds().get(0);
                // Get all adjacent edges from srcVertex to targetVertex
                List<EdgeTypeId> outEdges = schema.getEdgeTypes(srcVertexType, targetVertexType);
                for (EdgeTypeId outEdge : outEdges) {
                    if (srcVertexType.equals(outEdge.getSrcLabelId())) {
                        ExtendEdge extendEdge =
                                new ExtendEdge(srcVertexOrder, outEdge, PatternDirection.OUT);
                        if (extendEdgesWithDstType.containsKey(outEdge.getDstLabelId())) {
                            extendEdgesWithDstType.get(outEdge.getDstLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstType.put(
                                    outEdge.getDstLabelId(),
                                    new ArrayList<ExtendEdge>(Arrays.asList(extendEdge)));
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "In ExtendStep, srcVertexType "
                                        + srcVertexType
                                        + " is not equal to outEdge srcLabelId "
                                        + outEdge.getSrcLabelId());
                    }
                }
                // Get all adjacent edges from targetVertex to srcVertex
                List<EdgeTypeId> inEdges = schema.getEdgeTypes(targetVertexType, srcVertexType);
                for (EdgeTypeId inEdge : inEdges) {
                    if (srcVertexType.equals(inEdge.getDstLabelId())) {
                        ExtendEdge extendEdge =
                                new ExtendEdge(srcVertexOrder, inEdge, PatternDirection.IN);
                        if (extendEdgesWithDstType.containsKey(inEdge.getSrcLabelId())) {
                            extendEdgesWithDstType.get(inEdge.getSrcLabelId()).add(extendEdge);
                        } else {
                            extendEdgesWithDstType.put(
                                    inEdge.getSrcLabelId(),
                                    new ArrayList<ExtendEdge>(Arrays.asList(extendEdge)));
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "In ExtendStep, srcVertexType "
                                        + srcVertexType
                                        + " is not equal to inEdge dstLabelId "
                                        + inEdge.getDstLabelId());
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
                // A subset with duplicated edges. i.e., currently, we allow a pattern of person <->
                // person
                for (List<ExtendEdge> subset : subsets) {
                    extendSteps.add(new ExtendStep((Integer) entry.getKey(), subset));
                }
            }
        }

        return extendSteps;
    }

    /**
     * Extend current pattern with the given extendStep, and return the new pattern.
     * @param extendStep
     * @return new pattern
     */
    public Pattern extend(ExtendStep extendStep) {
        Pattern newPattern = new Pattern(this);
        Integer targetVertexTypeId = extendStep.getTargetVertexType();
        PatternVertex targetVertex =
                new SinglePatternVertex(targetVertexTypeId, newPattern.maxVertexId);
        newPattern.addVertex(targetVertex);
        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
            PatternDirection dir = extendEdge.getDirection();
            Integer srcVertexOrder = extendEdge.getSrcVertexOrder();
            PatternVertex srcVertex = newPattern.getVertexByOrder(srcVertexOrder);
            EdgeTypeId edgeTypeId = extendEdge.getEdgeTypeId();
            if (dir.equals(PatternDirection.OUT)) {
                PatternEdge edge =
                        new SinglePatternEdge(
                                srcVertex, targetVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(srcVertex, targetVertex, edge);
            } else {
                PatternEdge edge =
                        new SinglePatternEdge(
                                targetVertex, srcVertex, edgeTypeId, newPattern.maxEdgeId);
                newPattern.addEdge(targetVertex, srcVertex, edge);
            }
        }
        newPattern.reordering();
        return newPattern;
    }

    public void reordering() {
        PatternOrderCanonicalLabelingImpl patternOrder =
                new PatternOrderCanonicalLabelingImpl(this.patternGraph);
        this.patternOrder = patternOrder;
    }

    public boolean addVertex(Integer type) {
        PatternVertex vertex = new SinglePatternVertex(type, this.maxVertexId);
        return addVertex(vertex);
    }

    /**
     * Add a vertex into pattern
     * @param vertex
     * @return true if the vertex is added successfully into pattern graph and false otherwise
     */
    public boolean addVertex(PatternVertex vertex) {
        boolean added = this.patternGraph.addVertex(vertex);
        if (added) {
            this.maxVertexId++;
        }
        return added;
    }

    /**
     * Remove a vertex with its adjacent edges from pattern, and return the connected components of the remaining pattern.
     * @param vertex
     * @return the connected components of the remaining pattern
     */
    public List<Set<PatternVertex>> removeVertex(PatternVertex vertex) {
        boolean removed = this.patternGraph.removeVertex(vertex);
        if (removed) {
            this.maxVertexId = this.patternGraph.vertexSet().size();
            this.maxEdgeId = this.patternGraph.edgeSet().size();
            this.reordering();
        }
        return this.connectivityInspector.connectedSets();
    }

    public boolean removeEdge(PatternEdge edge, boolean removeVertex) {
        boolean removed = patternGraph.removeEdge(edge);
        if (removed) {
            if (removeVertex) {
                if (getEdgesOf(edge.getSrcVertex()).isEmpty()) {
                    patternGraph.removeVertex(edge.getSrcVertex());
                }
                if (getEdgesOf(edge.getDstVertex()).isEmpty()) {
                    patternGraph.removeVertex(edge.getDstVertex());
                }
                this.maxVertexId = this.patternGraph.vertexSet().size();
            }
            this.maxEdgeId = this.patternGraph.edgeSet().size();
            this.reordering();
        }
        return removed;
    }

    public boolean isConnected() {
        return this.connectivityInspector.connectedSets().size() == 1;
    }

    public List<Set<PatternVertex>> getConnectedComponents() {
        return this.connectivityInspector.connectedSets();
    }

    public int getDegree(PatternVertex vertex) {
        return this.patternGraph.degreeOf(vertex);
    }

    /**
     * Add an edge into pattern
     * @param srcVertex
     * @param dstVertex
     * @param edgeTypeId
     * @return true if the edge is added successfully into pattern graph and false otherwise
     */
    public boolean addEdge(
            PatternVertex srcVertex, PatternVertex dstVertex, EdgeTypeId edgeTypeId) {
        PatternEdge edge = new SinglePatternEdge(srcVertex, dstVertex, edgeTypeId, this.maxEdgeId);
        return addEdge(srcVertex, dstVertex, edge);
    }

    /**
     * Add an edge into pattern
     * @param srcVertex
     * @param dstVertex
     * @param edge
     * @return true if the edge is added successfully into pattern graph and false otherwise
     */
    public boolean addEdge(PatternVertex srcVertex, PatternVertex dstVertex, PatternEdge edge) {
        boolean added = this.patternGraph.addEdge(srcVertex, dstVertex, edge);
        if (added) {
            this.maxEdgeId++;
        }
        return added;
    }

    /**
     * Get the vertex with the given vertex id
     * @param vertexId
     * @return the vertex with the given vertex id
     */
    public PatternVertex getVertexById(Integer vertexId) {
        for (PatternVertex vertex : this.patternGraph.vertexSet()) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    /**
     * Get the vertex with the given vertex order
     * @param vertexId
     * @return the vertex with the given vertex order
     */
    public PatternVertex getVertexByOrder(int vertexId) {
        return this.patternOrder.getVertexByOrder(vertexId);
    }

    /**
     * Get the vertex order of the given vertex
     * @param vertex
     * @return the vertex order of the given vertex
     */
    public Integer getVertexOrder(PatternVertex vertex) {
        return this.patternOrder.getVertexOrder(vertex);
    }

    /**
     * Get the vertex group of the given vertex
     * @param vertex
     * @return the vertex group of the given vertex
     */
    public Integer getVertexGroup(PatternVertex vertex) {
        return this.patternOrder.getVertexGroup(vertex);
    }

    public int getPatternId() {
        return this.id;
    }

    public boolean containsVertex(PatternVertex vertex) {
        return this.patternGraph.containsVertex(vertex);
    }

    @Override
    public String toString() {
        return "Pattern "
                + this.id
                + " Vertices: "
                + this.patternGraph.vertexSet().toString()
                + ", PatternEdges: "
                + this.patternGraph.edgeSet().toString()
                + ", PatternOrder: "
                + this.patternOrder;
    }

    private boolean preCheck(Pattern other) {
        // compare vertex number and edge number
        if (this.maxVertexId != other.maxVertexId || this.maxEdgeId != other.maxEdgeId) {
            return false;
        }
        // compare pattern order
        if (this.patternOrder == null) {
            this.reordering();
        }
        if (other.patternOrder == null) {
            other.reordering();
        }
        if (!this.patternOrder.equals(other.patternOrder)) {
            return false;
        }
        return true;
    }

    public boolean isIsomorphicTo(Pattern other) {
        return isIsomorphicTo(other, vertexTypeComparator, edgeTypeComparator);
    }

    public boolean isIsomorphicTo(
            Pattern other,
            Comparator<PatternVertex> vertexComparator,
            Comparator<PatternEdge> edgeComparator) {
        if (this == other) {
            return true;
        } else if (!this.preCheck(other)) {
            return false;
        } else {
            VF2GraphIsomorphismInspector<PatternVertex, PatternEdge> isomorphismInspector =
                    new VF2GraphIsomorphismInspector<PatternVertex, PatternEdge>(
                            this.patternGraph,
                            other.patternGraph,
                            vertexComparator,
                            edgeComparator);
            return isomorphismInspector.isomorphismExists();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pattern) {
            Pattern other = (Pattern) obj;
            return isIsomorphicTo(other);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.maxVertexId, this.maxEdgeId, this.patternOrder);
    }
}
