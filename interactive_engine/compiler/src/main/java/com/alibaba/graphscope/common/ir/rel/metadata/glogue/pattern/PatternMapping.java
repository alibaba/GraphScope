package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import org.jgrapht.GraphMapping;

public class PatternMapping {
    GraphMapping<PatternVertex, PatternEdge> graphMapping;

    public PatternMapping(GraphMapping<PatternVertex, PatternEdge> graphMapping) {
        this.graphMapping = graphMapping;
    }

    public PatternVertex getMappedVertex(PatternVertex oriPatternVertex) {
        return graphMapping.getVertexCorrespondence(oriPatternVertex, true);
    }

    public PatternVertex getOriPatternVertex(PatternVertex mappedPatternVertex) {
        return graphMapping.getVertexCorrespondence(mappedPatternVertex, false);
    }
}
