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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GlogueBasicCardinalityEstimationImpl implements GlogueCardinalityEstimation {
    private Map<Pattern, Double> patternCardinality;
    private static Logger logger =
            LoggerFactory.getLogger(GlogueBasicCardinalityEstimationImpl.class);

    public GlogueBasicCardinalityEstimationImpl(Glogue glogue, GlogueSchema schema) {
        this.patternCardinality = new HashMap<Pattern, Double>();
        create(glogue, schema);
    }

    private GlogueBasicCardinalityEstimationImpl create(Glogue glogue, GlogueSchema schema) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        List<Pattern> roots = glogue.getRoots();
        for (Pattern pattern : roots) {
            // single vertex pattern
            PatternVertex singleVertexPattern = pattern.getVertexSet().iterator().next();
            if (singleVertexPattern.getVertexTypeIds().size() != 1) {
                throw new UnsupportedOperationException(
                        "In GlogueBasicCardinalityEstimationImpl creation, singleVertexPattern "
                                + singleVertexPattern
                                + " is not of basic type.");
            }
            Integer vertexTypeId = singleVertexPattern.getVertexTypeIds().get(0);
            Double singleVertexPatternCount = schema.getVertexTypeCardinality(vertexTypeId);
            this.patternCardinality.put(pattern, singleVertexPatternCount);
            patternQueue.add(pattern);
        }

        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            Double patternCount = this.patternCardinality.get(pattern);
            for (GlogueEdge edge : glogue.getGlogueOutEdges(pattern)) {
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern newPattern = extendIntersectEdge.getDstPattern();
                ExtendStep extendStep = extendIntersectEdge.getExtendStep();

                if (this.containsPattern(newPattern)) {
                    // if the cardinality of the pattern is already computed previously, compute the
                    // pattern extension cost.
                    Double extendStepWeight = estimateExtendWeight(schema, extendStep);
                    extendStep.setWeight(extendStepWeight);
                } else {
                    // otherwise, compute the cardinality of the pattern, together with the pattern
                    // extension cost.
                    Pair<Double, Double> patternCountWithWeight =
                            estimatePatternCountWithExtendWeight(schema, patternCount, extendStep);
                    this.patternCardinality.put(newPattern, patternCountWithWeight.getValue0());
                    extendStep.setWeight(patternCountWithWeight.getValue1());
                    patternQueue.add(newPattern);
                }
            }
        }

        return this;
    }

    /// Given the src pattern and extend step, estimate the cardinality of the
    /// target pattern by extending the extendStep from srcPattern, together with
    /// pattern extension cost.
    /// Return the pair of (targetPatternCardinality, extendStepWeight)
    private Pair<Double, Double> estimatePatternCountWithExtendWeight(
            GlogueSchema schema, Double srcPatternCount, ExtendStep extendStep) {
        initEdgeWeightsInExtendStep(schema, extendStep);
        // estimate pattern count and the weight of the extend step
        Double commonTargetVertexTypeCount =
                schema.getVertexTypeCardinality(extendStep.getTargetVertexType());
        Iterator<ExtendEdge> iter = extendStep.getExtendEdges().iterator();
        Double targetPatternCount = srcPatternCount * iter.next().getWeight();
        Double intermediate = targetPatternCount;
        while (iter.hasNext()) {
            ExtendEdge extendEdge = iter.next();
            targetPatternCount *= extendEdge.getWeight() / commonTargetVertexTypeCount;
            intermediate += targetPatternCount;
        }
        return Pair.with(targetPatternCount, intermediate / srcPatternCount);
    }

    /// Given the src pattern and extend step, estimate the pattern extension cost.
    /// Return the estimated extendStepWeight.
    /// Currently, we compute the weight of the extend step in a greedy way;
    /// we can also compute it in a more accurate way (e.g., enumerate all
    /// combinations and get the best one)
    private Double estimateExtendWeight(GlogueSchema schema, ExtendStep extendStep) {
        initEdgeWeightsInExtendStep(schema, extendStep);
        Double commonTargetVertexCount =
                schema.getVertexTypeCardinality(extendStep.getTargetVertexType());
        Iterator<ExtendEdge> iter = extendStep.getExtendEdges().iterator();
        Double extendStepWeight = iter.next().getWeight();
        Double intermediate = 1.0;
        while (iter.hasNext()) {
            ExtendEdge extendEdge = iter.next();
            intermediate *= extendEdge.getWeight() / commonTargetVertexCount;
            extendStepWeight += intermediate;
        }
        return extendStepWeight;
    }

    private void initEdgeWeightsInExtendStep(GlogueSchema schema, ExtendStep extendStep) {
        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
            // each extendEdge extends to a new edge
            // estimate the cardinality by multiplying the edge type cardinality
            Double extendEdgeCount = schema.getEdgeTypeCardinality(extendEdge.getEdgeTypeId());
            // each srcVertex is a common vertex when extending the edge
            // estimate the cardinality by dividing the src vertex type cardinality
            Integer srcVertexType;
            if (extendEdge.getDirection().equals(PatternDirection.OUT)) {
                srcVertexType = extendEdge.getEdgeTypeId().getSrcLabelId();
            } else {
                srcVertexType = extendEdge.getEdgeTypeId().getDstLabelId();
            }
            Double commonSrcVertexCount = schema.getVertexTypeCardinality(srcVertexType);
            // Set the ExtendEdge weight: the estimated average pattern cardinality after
            // extending current expand edge
            extendEdge.setWeight(extendEdgeCount / commonSrcVertexCount);
        }
        extendStep.sortExtendEdges();
    }

    private boolean containsPattern(Pattern pattern) {
        return this.patternCardinality.containsKey(pattern);
    }

    @Override
    public Double getCardinality(Pattern queryPattern) {
        return getCardinality(queryPattern, false);
    }

    public @Nullable Double getCardinality(Pattern queryPattern, boolean allowsNull) {
        for (Pattern pattern : this.patternCardinality.keySet()) {
            if (pattern.equals(queryPattern)) {
                return this.patternCardinality.get(pattern);
            }
        }
        if (allowsNull) {
            return null;
        }
        // if not exist, return 1.0
        logger.warn("pattern {} not found in glogue, return count = 1.0", queryPattern);
        return 1.0;
    }

    @Override
    public String toString() {
        String s = "";
        for (Pattern pattern : this.patternCardinality.keySet()) {
            s +=
                    "Pattern "
                            + pattern.getPatternId()
                            + ": "
                            + this.patternCardinality.get(pattern)
                            + "\n";
        }
        return s;
    }
}
