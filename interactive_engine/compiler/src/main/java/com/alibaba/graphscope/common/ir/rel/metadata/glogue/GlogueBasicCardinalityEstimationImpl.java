package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.Map;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternOrder;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class GlogueBasicCardinalityEstimationImpl implements GlogueCardinalityEstimation {
    private Map<Pattern, Double> patternCardinality;

    public GlogueBasicCardinalityEstimationImpl() {
        this.patternCardinality = new HashMap<Pattern, Double>();
    }

    public GlogueBasicCardinalityEstimationImpl create(Glogue glogue, GlogueSchema schema) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        List<Pattern> roots = glogue.getRoots();
        for (Pattern pattern : roots) {
            if (pattern.getVertexSet().size() == 1) {
                // single vertex pattern
                Integer singleVertexPatternType = pattern.getVertexSet().iterator().next().getVertexTypeId();
                Double singleVertexPatternCount = schema.getVertexTypeCardinality(singleVertexPatternType);
                this.patternCardinality.put(pattern, singleVertexPatternCount);
                System.out.println("root vertex pattern: " + pattern + ": " + singleVertexPatternCount);
            }
            for (GlogueEdge edge : glogue.getOutEdges(pattern)) {
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern singleEdgePattern = extendIntersectEdge.getDstPattern();
                // if it is already computed previously, then skip.
                if (this.containsPattern(singleEdgePattern)) {
                    System.out.println("pattern already computed: " + singleEdgePattern);
                    continue;
                }
                if (singleEdgePattern.getEdgeSet().size() == 1) {
                    // single edge pattern
                    EdgeTypeId singleEdgePatternType = singleEdgePattern.getEdgeSet().iterator().next().getEdgeTypeId();
                    Double singleEdgePatternCount = schema.getEdgeTypeCardinality(singleEdgePatternType);
                    this.patternCardinality.put(singleEdgePattern, singleEdgePatternCount);
                    patternQueue.add(singleEdgePattern);
                    System.out.println("root edge pattern: " + singleEdgePattern + ": " + singleEdgePatternCount);
                } else {
                    System.out.println("TODO: root edge pattern with multiple edges " + singleEdgePattern);
                }
            }
        }

        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            System.out.println("~~~~~~~~pop pattern in queue~~~~~~~~~~");
            System.out.println("original pattern " + pattern);
            for (GlogueEdge edge : glogue.getOutEdges(pattern)) {
                // each GlogueEdge extends to a new pattern
                // initial as current pattern count
                Double estimatedPatternCount = this.patternCardinality.get(pattern);
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern newPattern = extendIntersectEdge.getDstPattern();
                if (this.containsPattern(newPattern)) {
                    System.out.println("pattern already computed: " + newPattern);
                    continue;
                }
                ExtendStep extendStep = extendIntersectEdge.getExtendStep();
                System.out.println("extend step: " + extendStep.toString());
                List<EdgeTypeId> extendEdges = extractExtendEdgeTypes(extendStep);
                System.out.println("extend edge types: " + extendEdges);
                for (EdgeTypeId extendEdge : extendEdges) {
                    Double edgeTypeCount = schema.getEdgeTypeCardinality(extendEdge);
                    estimatedPatternCount *= edgeTypeCount;
                }
                // commonVertices includes all src vertices, and, if the step has more than one
                // extend edge,
                // then the target vertex is also a common vertex.
                List<Integer> commonVertices = extractExtendSrcVertices(extendStep);
                System.out.println("common src vertices types: " + commonVertices);
                for (Integer commonVertex : commonVertices) {
                    Double vertexTypeCount = schema.getVertexTypeCardinality(commonVertex);
                    estimatedPatternCount /= vertexTypeCount;
                }
                int count = extendStep.getExtendEdges().size();
                while (count > 1) {
                    Integer commonTargetVertex = extendStep.getTargetVertexType();
                    estimatedPatternCount /= schema.getVertexTypeCardinality(commonTargetVertex);
                    count -= 1;
                }
                this.patternCardinality.put(newPattern, estimatedPatternCount);
                patternQueue.add(newPattern);
                System.out.println("new pattern: " + newPattern + ": " + estimatedPatternCount);
            }
        }

        return this;
    }

    private List<EdgeTypeId> extractExtendEdgeTypes(ExtendStep extendStep) {
        List<EdgeTypeId> edgeTypeIdList = new ArrayList<>();
        for (ExtendEdge edge : extendStep.getExtendEdges()) {
            edgeTypeIdList.add(edge.getEdgeTypeId());
        }
        return edgeTypeIdList;
    }

    public List<Integer> extractExtendSrcVertices(ExtendStep extendStep) {
        List<Integer> vertexTypeIdList = new ArrayList<>();
        for (ExtendEdge edge : extendStep.getExtendEdges()) {
            if (edge.getDirection().equals(PatternDirection.OUT)) {
                vertexTypeIdList.add(edge.getEdgeTypeId().getSrcLabelId());
            } else {
                vertexTypeIdList.add(edge.getEdgeTypeId().getDstLabelId());
            }
        }
        return vertexTypeIdList;
    }

    private boolean containsPattern(Pattern pattern) {
        return this.patternCardinality.containsKey(pattern);
    }

    @Override
    public double getCardinality(Pattern queryPattern) {
        for (Pattern pattern : this.patternCardinality.keySet()) {
            if (pattern.equals(queryPattern)) {
                return this.patternCardinality.get(pattern);
            }
        }
        return 0.0;
    }

    @Override
    public String toString() {
        String s = "";
        for (Pattern pattern : this.patternCardinality.keySet()) {
            s += pattern + ": " + this.patternCardinality.get(pattern) + "\n";
        }
        return s;
    }

}
