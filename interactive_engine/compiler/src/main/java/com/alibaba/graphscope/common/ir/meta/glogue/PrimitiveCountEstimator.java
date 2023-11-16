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
            return gq.getRowCount(pattern);
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
            sum += gq.getRowCount(new Pattern(new SinglePatternVertex(typeId)));
        }
        return sum * vertex.getDetails().getSelectivity();
    }

    public double estimate(PatternEdge edge) {
        double sum = 0.0d;
        for (EdgeTypeId typeId : edge.getEdgeTypeIds()) {
            Pattern edgePattern = new Pattern();
            PatternVertex srcVertex = new SinglePatternVertex(typeId.getSrcLabelId(), 0);
            PatternVertex dstVertex = new SinglePatternVertex(typeId.getDstLabelId(), 1);
            edgePattern.addVertex(srcVertex);
            edgePattern.addVertex(dstVertex);
            edgePattern.addEdge(
                    srcVertex, dstVertex, new SinglePatternEdge(srcVertex, dstVertex, typeId, 0));
            sum += gq.getRowCount(edgePattern);
        }
        if (edge.isBoth()) {
            sum *= 2;
        }
        PatternVertex src = edge.getSrcVertex();
        PatternVertex dst = edge.getDstVertex();
        return sum
                * edge.getDetails().getSelectivity()
                * src.getDetails().getSelectivity()
                * dst.getDetails().getSelectivity();
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
