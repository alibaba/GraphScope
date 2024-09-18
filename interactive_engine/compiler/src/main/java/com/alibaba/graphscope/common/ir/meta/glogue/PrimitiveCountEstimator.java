/*
 * Copyright 2020 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class PrimitiveCountEstimator {
    private final GlogueQuery gq;

    public PrimitiveCountEstimator(GlogueQuery gq) {
        this.gq = gq;
    }

    public @Nullable Double estimate(Pattern pattern) {
        if (Utils.canLookUpFromGlogue(pattern, gq.getMaxPatternSize())) {
            Double countFromGlogue = gq.getRowCount(pattern, true);
            if (countFromGlogue != null) {
                return countFromGlogue;
            }
        }
        // estimate the pattern graph with intersect, i.e. a->b, c->b, d->b
        PatternVertex intersect = getIntersectVertex(pattern);
        if (intersect != null) {
            Set<PatternEdge> edges = pattern.getEdgesOf(intersect);
            if (edges.isEmpty()) {
                return estimate(intersect);
            }
            double count = 1.0d;
            for (PatternEdge edge : edges) {
                count *= estimate(edge);
            }
            double intersectCount = estimate(intersect);
            count /= Math.pow(intersectCount, edges.size() - 1);
            return count;
        }
        return null;
    }

    public double estimate(PatternVertex vertex) {
        double sum = 0.0d;
        for (Integer typeId : vertex.getVertexTypeIds()) {
            sum += gq.getRowCount(new Pattern(new SinglePatternVertex(typeId)), false);
        }
        return sum * vertex.getElementDetails().getSelectivity();
    }

    public double estimate(PatternEdge edge) {
        double sum = 0.0d;
        for (EdgeTypeId typeId : edge.getEdgeTypeIds()) {
            sum += estimate(edge, typeId);
        }
        if (edge.isBoth()) {
            sum *= 2;
        }
        PatternVertex src = edge.getSrcVertex();
        PatternVertex dst = edge.getDstVertex();
        // todo: for path expand, the edge selectivity should be calculated in each hop, and the
        // vertex selectivity should be calculated in each hop if the filter condition is applied in
        // each hop
        return sum
                * edge.getElementDetails().getSelectivity()
                * src.getElementDetails().getSelectivity()
                * dst.getElementDetails().getSelectivity();
    }

    // estimate the row count for each type id in the edge
    public double estimate(PatternEdge edge, EdgeTypeId typeId) {
        int minHop = 1, maxHop = 1;
        PathExpandRange range = edge.getElementDetails().getRange();
        if (range != null) {
            minHop = range.getOffset();
            maxHop = range.getOffset() + range.getFetch() - 1;
        }
        double sum = 0.0d;
        for (int hop = minHop; hop <= maxHop; ++hop) {
            sum += estimate(edge, typeId, hop);
        }
        return sum;
    }

    public double estimate(PatternEdge edge, EdgeTypeId typeId, int hops) {
        PatternVertex srcVertex = new SinglePatternVertex(typeId.getSrcLabelId(), 0);
        PatternVertex dstVertex = new SinglePatternVertex(typeId.getDstLabelId(), 1);
        if (hops == 0) {
            return estimate(srcVertex);
        }
        Pattern edgePattern = new Pattern();
        edgePattern.addVertex(srcVertex);
        edgePattern.addVertex(dstVertex);
        edgePattern.addEdge(
                srcVertex, dstVertex, new SinglePatternEdge(srcVertex, dstVertex, typeId, 0));
        edgePattern.reordering();
        return Math.pow(gq.getRowCount(edgePattern, false), hops)
                / Math.pow(estimate(dstVertex), hops - 1);
    }

    private @Nullable PatternVertex getIntersectVertex(Pattern pattern) {
        int edgeNum = pattern.getEdgeNumber();
        for (PatternVertex vertex : pattern.getVertexSet()) {
            if (pattern.getDegree(vertex) == edgeNum) {
                return vertex;
            }
        }
        return null;
    }
}
