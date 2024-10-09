/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.google.common.base.Preconditions;

import org.checkerframework.checker.nullness.qual.Nullable;

// an interface to define the cost estimation of an edge
public abstract class EdgeCostEstimator<T> {
    protected final CountHandler handler;

    public EdgeCostEstimator(CountHandler handler) {
        this.handler = handler;
    }

    public abstract T estimate(Pattern srcPattern, PatternEdge edge, PatternVertex target);

    // join-based cost estimator
    public static class Join extends EdgeCostEstimator<ExpandJoin> {
        public Join(CountHandler handler) {
            super(handler);
        }

        // todo: support scenario for relational DB, i.e. DuckDB which has graph index or foreign
        // key optimization techniques
        @Override
        public ExpandJoin estimate(Pattern srcPattern, PatternEdge edge, PatternVertex target) {
            return null;
        }
    }

    // extend-based cost estimator
    public static class Extend extends EdgeCostEstimator<DetailedExpandCost> {
        public Extend(CountHandler handler) {
            super(handler);
        }

        @Override
        public DetailedExpandCost estimate(
                @Nullable Pattern srcPattern, PatternEdge edge, PatternVertex target) {
            PatternVertex src = Utils.getExtendFromVertex(edge, target);
            DetailedExpandCost edgeCost = estimateEdge(edge, src, target);
            if (srcPattern == null) return edgeCost;
            double srcPatternCount = handler.handle(srcPattern);
            double srcIntersectCount = getIntersectCount(src);
            double targetIntersectCount = getTargetIntersectCount(srcPattern, target);
            return new DetailedExpandCost(
                    getExpandCost(
                            edgeCost.getExpandRows(),
                            srcPatternCount,
                            srcIntersectCount,
                            targetIntersectCount),
                    getExpandCost(
                            edgeCost.getExpandFilteringRows(),
                            srcPatternCount,
                            srcIntersectCount,
                            targetIntersectCount),
                    getExpandCost(
                            edgeCost.getGetVRows(),
                            srcPatternCount,
                            srcIntersectCount,
                            targetIntersectCount),
                    getExpandCost(
                            edgeCost.getGetVFilteringRows(),
                            srcPatternCount,
                            srcIntersectCount,
                            targetIntersectCount));
        }

        private double getExpandCost(
                double edgeRows,
                double srcPatternCount,
                double srcIntersectCount,
                double targetIntersectCount) {
            Preconditions.checkArgument(
                    Double.compare(srcIntersectCount, 0.0d) != 0,
                    "srcIntersectCount should not be 0");
            Preconditions.checkArgument(
                    Double.compare(targetIntersectCount, 0.0d) != 0,
                    "targetIntersectCount should not be 0");
            return Math.max(
                    edgeRows * (srcPatternCount / srcIntersectCount / targetIntersectCount), 1.0d);
        }

        private double getIntersectCount(PatternVertex vertex) {
            return handler.handle(new Pattern(vertex));
        }

        private double getTargetIntersectCount(Pattern pattern, PatternVertex target) {
            if (!pattern.containsVertex(target)) {
                return 1.0d;
            }
            return getIntersectCount(target);
        }

        private DetailedExpandCost estimateEdge(
                PatternEdge edge, PatternVertex src, PatternVertex target) {
            double targetSelectivity = target.getElementDetails().getSelectivity();
            if (Double.compare(targetSelectivity, 1.0d) != 0) {
                target =
                        (target instanceof SinglePatternVertex)
                                ? new SinglePatternVertex(
                                        target.getVertexTypeIds().get(0), target.getId())
                                : new FuzzyPatternVertex(target.getVertexTypeIds(), target.getId());
            }
            double edgeSelectivity = edge.getElementDetails().getSelectivity();
            if (Double.compare(targetSelectivity, 1.0d) != 0
                    || Double.compare(edgeSelectivity, 1.0d) != 0) {
                PatternVertex edgeSrc = (src == edge.getSrcVertex()) ? src : target;
                PatternVertex edgeDst = (src == edge.getSrcVertex()) ? target : src;
                edge =
                        edge instanceof SinglePatternEdge
                                ? new SinglePatternEdge(
                                        edgeSrc,
                                        edgeDst,
                                        edge.getEdgeTypeIds().get(0),
                                        edge.getId(),
                                        edge.isBoth(),
                                        createDetailsWithNoFilter(edge.getElementDetails()))
                                : new FuzzyPatternEdge(
                                        edgeSrc,
                                        edgeDst,
                                        edge.getEdgeTypeIds(),
                                        edge.getId(),
                                        edge.isBoth(),
                                        createDetailsWithNoFilter(edge.getElementDetails()));
            }
            Pattern pattern = new Pattern();
            pattern.addVertex(edge.getSrcVertex());
            pattern.addVertex(edge.getDstVertex());
            pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
            double patternCost = handler.handle(pattern);
            double expandCost =
                    patternCost
                            + handler.labelConstraintsDeltaCost(edge, target)
                                    * src.getElementDetails().getSelectivity();
            double expandFilteringCost = expandCost * edgeSelectivity;
            double getVCost = patternCost * edgeSelectivity;
            double getVFilteringCost = getVCost * targetSelectivity;
            return new DetailedExpandCost(
                    expandCost, expandFilteringCost, getVCost, getVFilteringCost);
        }

        private ElementDetails createDetailsWithNoFilter(ElementDetails original) {
            return new ElementDetails(
                    1.0d,
                    original.getRange(),
                    original.getPxdInnerGetVTypes(),
                    original.getResultOpt(),
                    original.getPathOpt(),
                    original.isOptional());
        }
    }
}
