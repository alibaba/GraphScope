package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.Map;

import org.javatuples.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

import com.alibaba.graphscope.common.ir.rel.graph.pattern.PatternCode;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils.Combinations;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class GlogueBasicCardinalityEstimationImpl implements GlogueCardinalityEstimation {
    private Map<PatternCode, Double> patternCardinality;

    public GlogueBasicCardinalityEstimationImpl() {
        this.patternCardinality = new HashMap<PatternCode, Double>();
    }

    public GlogueBasicCardinalityEstimationImpl create(Glogue glogue, GlogueSchema schema) {
        Deque<Pair<Pattern, PatternCode>> patternQueue = new ArrayDeque<>();
        List<PatternCode> roots = glogue.getRoots();
        for (PatternCode patternCode : roots) {
            Pattern pattern = glogue.getPatternByCode(patternCode);
            if (pattern.getVertexSet().size() == 1) {
                // single vertex pattern
                Integer singleVertexPatternType = pattern.getVertexSet().iterator().next().getVertexTypeId();
                Double singleVertexPatternCount = schema.getVertexTypeCardinality(singleVertexPatternType);
                this.patternCardinality.put(patternCode, singleVertexPatternCount);
                System.out.println("root vertex pattern: " + patternCode + ": " + singleVertexPatternCount);
            }
            for (GlogueEdge edge : glogue.getOutEdges(pattern)) {
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern singleEdgePattern = extendIntersectEdge.getDstPattern();
                PatternCode singleEdgePatternCode = singleEdgePattern.encoding();
                // if it is already computed previously, then skip.
                if (this.containsPatternCode(singleEdgePatternCode)) {
                    continue;
                }
                if (singleEdgePattern.getEdgeSet().size() == 1) {
                    // single edge pattern
                    EdgeTypeId singleEdgePatternType = singleEdgePattern.getEdgeSet().iterator().next().getEdgeTypeId();
                    Double singleEdgePatternCount = schema.getEdgeTypeCardinality(singleEdgePatternType);
                    this.patternCardinality.put(singleEdgePatternCode, singleEdgePatternCount);
                    patternQueue.add(Pair.with(singleEdgePattern, singleEdgePatternCode));
                    System.out.println("root edge pattern: " + singleEdgePatternCode + ": " + singleEdgePatternCount);
                } else {
                    System.out.println("TODO: root edge pattern with multiple edges " + singleEdgePattern);
                }
            }
        }

        while (patternQueue.size() > 0) {
            Pair<Pattern, PatternCode> patternInfo = patternQueue.pop();
            System.out.println("~~~~~~~~pop pattern in queue~~~~~~~~~~");
            System.out.println("original pattern " + patternInfo.getValue0());
            for (GlogueEdge edge : glogue.getOutEdges(patternInfo.getValue0())) {
                // each GlogueEdge extends to a new pattern
                // initial as current pattern count
                Double estimatedPatternCount = this.patternCardinality.get(patternInfo.getValue1());
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern newPattern = extendIntersectEdge.getDstPattern();
                PatternCode newPatternCode = newPattern.encoding();
                // if it is already computed previously, then skip.
                // TODO: we can also compute again, and update the previous count value, since
                // maybe the previous count is not accurate;
                // e.g., if the new value is smaller, we may update it with the smaller value.
                // Or, they must be the same???
                if (this.containsPatternCode(newPatternCode)) {
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
                // commonVertices includes all src vertices, and, if the step has more than one extend edge,
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

                this.patternCardinality.put(newPatternCode, estimatedPatternCount);
                patternQueue.add(Pair.with(newPattern, newPatternCode));
                System.out.println("new pattern: " + newPatternCode + ": " + estimatedPatternCount);
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

    private boolean containsPatternCode(PatternCode patternCode) {
        // TODO: should be based on pattern code, i.e.,
        // return this.patternCardinality.containsKey(patternCode);
        for (PatternCode key : this.patternCardinality.keySet()) {
            // TODO: this validation is as expected. but directly validate by containsKey is
            // not working.
            if (key.equals(patternCode)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public double getCardinality(PatternCode patternCode) {
        throw new UnsupportedOperationException("Unimplemented method 'getCardinality'");
    }

    @Override
    public String toString() {
        return "GlogueBasicCardinalityEstimationImpl{" +
                "patternCardinality=" + patternCardinality +
                '}';
    }

}
