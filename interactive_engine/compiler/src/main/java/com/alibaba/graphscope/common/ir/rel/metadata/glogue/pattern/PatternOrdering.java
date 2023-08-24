package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import org.jgrapht.Graph;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.GraphOrdering;

// PatternOrdering, preserves the order of vertices in a pattern
// It is used for determining pattern mappings
public class PatternOrdering {
    GraphOrdering graphOrdering;

    public PatternOrdering(Graph<PatternVertex, PatternEdge> patternGraph) {
        this.graphOrdering = new GraphOrdering(patternGraph, true, false);
    }

    public Integer getVertexId(PatternVertex vertex) {
        return graphOrdering.getMapVertexToOrder().get(vertex);
    }

    public PatternVertex getVertexById(Integer id) {
        return graphOrdering.getMapOrderToVertex().get(id);
    }

    @Override
    public String toString() {
        return graphOrdering.toString();
    }
}
