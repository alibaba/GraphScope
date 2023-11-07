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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.glogue.ExtendWeightEstimator;
import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class ExtendIntersectRule<C extends ExtendIntersectRule.Config> extends RelRule<C> {

    protected ExtendIntersectRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        List<GraphExtendIntersect> edges =
                getExtendIntersectEdges(
                        call.rel(0), (GraphRelMetadataQuery) call.getMetadataQuery());
        for (GraphExtendIntersect edge : edges) {
            call.transformTo(edge);
        }
    }

    private List<GraphExtendIntersect> getExtendIntersectEdges(
            GraphPattern graphPattern, GraphRelMetadataQuery mq) {
        HeuristicComparator comparator = new HeuristicComparator(graphPattern);
        ExtendWeightEstimator estimator =
                new ExtendWeightEstimator(
                        (Pattern pattern) ->
                                mq.getRowCount(
                                        new GraphPattern(
                                                graphPattern.getCluster(),
                                                graphPattern.getTraitSet(),
                                                pattern)));
        Pattern pattern = graphPattern.getPattern();
        int patternSize = pattern.getVertexNumber();
        int maxPatternSize = config.getMaxPatternSizeInGlogue();
        List<GraphExtendIntersect> edges = Lists.newArrayList();
        if (patternSize <= 1) {
            return edges;
        }
        if (patternSize <= maxPatternSize) {
            Set<GlogueEdge> glogueEdges = mq.getGlogueEdges(graphPattern);
            glogueEdges.forEach(
                    k ->
                            edges.add(
                                    new GraphExtendIntersect(
                                            graphPattern.getCluster(),
                                            graphPattern.getTraitSet(),
                                            new GraphPattern(
                                                    graphPattern.getCluster(),
                                                    graphPattern.getTraitSet(),
                                                    k.getSrcPattern()),
                                            (GlogueExtendIntersectEdge) k)));
        } else {
            for (PatternVertex vertex : pattern.getVertexSet()) {
                Pattern clone = new Pattern(pattern);
                clone.setPatternId(UUID.randomUUID().hashCode());
                List connectedSets = clone.removeVertex(vertex);
                if (connectedSets.size() != 1) {
                    continue;
                }
                edges.add(createExtendIntersect(graphPattern, clone, pattern, vertex, estimator));
            }
        }
        Collections.sort(edges, comparator.getEdgeComparator());
        return edges;
    }

    private GraphExtendIntersect createExtendIntersect(
            GraphPattern graphPattern,
            Pattern src,
            Pattern dst,
            PatternVertex target,
            ExtendWeightEstimator estimator) {
        List<PatternEdge> adjacentEdges = Lists.newArrayList(dst.getEdgesOf(target));
        double totalWeight = estimator.estimate(adjacentEdges, target);
        List<ExtendEdge> extendEdges =
                adjacentEdges.stream()
                        .map(
                                k -> {
                                    List<EdgeTypeId> edgeTypeIds = k.getEdgeTypeIds();
                                    Preconditions.checkArgument(
                                            edgeTypeIds.size() == 1,
                                            "union types is unsupported yet in edge");
                                    PatternVertex extendFrom = Utils.getExtendFromVertex(k, target);
                                    return new ExtendEdge(
                                            src.getVertexOrder(extendFrom),
                                            edgeTypeIds.get(0),
                                            Utils.getExtendDirection(k, target),
                                            estimator.estimate(k, target));
                                })
                        .collect(Collectors.toList());
        List<Integer> vertexTypeIds = target.getVertexTypeIds();
        Preconditions.checkArgument(
                vertexTypeIds.size() == 1, "union types is unsupported yet in vertex");
        ExtendStep extendStep =
                new ExtendStep(
                        vertexTypeIds.get(0), dst.getVertexOrder(target), extendEdges, totalWeight);
        GlogueExtendIntersectEdge glogueEdge =
                new GlogueExtendIntersectEdge(src, dst, extendStep, getOrderMapping(src, dst));
        return new GraphExtendIntersect(
                graphPattern.getCluster(),
                graphPattern.getTraitSet(),
                new GraphPattern(graphPattern.getCluster(), graphPattern.getTraitSet(), src),
                glogueEdge);
    }

    private Map<Integer, Integer> getOrderMapping(Pattern src, Pattern dst) {
        Map<Integer, Integer> srcToDstOrderMap = Maps.newHashMap();
        for (PatternVertex vertex : src.getVertexSet()) {
            Integer dstOrder = dst.getVertexOrder(vertex);
            Preconditions.checkArgument(
                    dstOrder != null, "vertex %s is not in dst pattern %s", vertex, dst);
            srcToDstOrderMap.put(src.getVertexOrder(vertex), dstOrder);
        }
        return srcToDstOrderMap;
    }

    private static class HeuristicComparator {
        private final GraphPattern graphPattern;

        public HeuristicComparator(GraphPattern graphPattern) {
            this.graphPattern = graphPattern;
        }

        // order vertex by degree in descending way
        public Comparator<PatternVertex> getVertexComparator() {
            Pattern pattern = graphPattern.getPattern();
            return (PatternVertex v1, PatternVertex v2) ->
                    pattern.getDegree(v2) - pattern.getDegree(v1);
        }

        // order edge by weight in ascending way
        public Comparator<GraphExtendIntersect> getEdgeComparator() {
            return Comparator.comparingDouble(
                    (GraphExtendIntersect i) -> i.getGlogueEdge().getExtendStep().getWeight());
        }
    }

    public static class Config implements RelRule.Config {
        public static ExtendIntersectRule.Config DEFAULT =
                new ExtendIntersectRule.Config()
                        .withOperandSupplier(b0 -> b0.operand(GraphPattern.class).anyInputs());

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;
        private int maxPatternSizeInGlogue;

        @Override
        public RelRule toRule() {
            return new ExtendIntersectRule(this);
        }

        @Override
        public ExtendIntersectRule.Config withRelBuilderFactory(
                RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public ExtendIntersectRule.Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public ExtendIntersectRule.Config withOperandSupplier(OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        public ExtendIntersectRule.Config withMaxPatternSizeInGlogue(int maxPatternSizeInGlogue) {
            this.maxPatternSizeInGlogue = maxPatternSizeInGlogue;
            return this;
        }

        @Override
        public OperandTransform operandSupplier() {
            return this.operandSupplier;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
            return this.description;
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            return this.builderFactory;
        }

        public int getMaxPatternSizeInGlogue() {
            return maxPatternSizeInGlogue;
        }
    }
}
