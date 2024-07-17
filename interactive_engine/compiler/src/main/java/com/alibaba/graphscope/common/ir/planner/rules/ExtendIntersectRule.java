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

import com.alibaba.graphscope.common.ir.meta.glogue.CountHandler;
import com.alibaba.graphscope.common.ir.meta.glogue.ExtendWeightEstimator;
import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.*;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.Predicate;
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
                        new CountHandler() {
                            @Override
                            public double handle(Pattern pattern) {
                                return mq.getRowCount(
                                        new GraphPattern(
                                                graphPattern.getCluster(),
                                                graphPattern.getTraitSet(),
                                                pattern));
                            }

                            @Override
                            public double labelConstraintsDeltaCost(
                                    PatternEdge edge, PatternVertex target) {
                                return config.labelConstraintsEnabled()
                                        ? mq.getGlogueQuery()
                                                .getLabelConstraintsDeltaCost(edge, target)
                                        : 0.0d;
                            }
                        });
        Pattern pattern = graphPattern.getPattern();
        int patternSize = pattern.getVertexNumber();
        List<GraphExtendIntersect> edges = Lists.newArrayList();
        if (patternSize <= 1) {
            return edges;
        }
        PruningStrategy pruningStrategy = new PruningStrategy(pattern);
        for (PatternVertex vertex : pattern.getVertexSet()) {
            if (pruningStrategy.toPrune(vertex)) {
                continue;
            }
            edges.add(createExtendIntersect(graphPattern, vertex, estimator));
        }
        Collections.sort(edges, comparator.getEdgeComparator());
        return edges;
    }

    private GraphExtendIntersect createExtendIntersect(
            GraphPattern graphPattern, PatternVertex target, ExtendWeightEstimator estimator) {
        Pattern dst = graphPattern.getPattern();
        Pattern src = new Pattern(dst);
        src.setPatternId(UUID.randomUUID().hashCode());
        src.removeVertex(target);
        List<PatternEdge> adjacentEdges = Lists.newArrayList(dst.getEdgesOf(target));
        double totalWeight = estimator.estimate(adjacentEdges, target);
        List<ExtendEdge> extendEdges =
                adjacentEdges.stream()
                        .map(
                                k -> {
                                    PatternVertex extendFrom = Utils.getExtendFromVertex(k, target);
                                    return new ExtendEdge(
                                            src.getVertexOrder(extendFrom),
                                            k.getEdgeTypeIds(),
                                            Utils.getExtendDirection(k, target),
                                            estimator.estimate(k, target),
                                            k.getElementDetails());
                                })
                        .collect(Collectors.toList());
        ExtendStep extendStep =
                new ExtendStep(
                        target.getVertexTypeIds(),
                        dst.getVertexOrder(target),
                        extendEdges,
                        totalWeight);
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

        // order edge by weight in ascending way and then by target vertex id in descending way
        public Comparator<GraphExtendIntersect> getEdgeComparator() {
            return (GraphExtendIntersect e1, GraphExtendIntersect e2) -> {
                ExtendStep step1 = e1.getGlogueEdge().getExtendStep();
                ExtendStep step2 = e2.getGlogueEdge().getExtendStep();
                int compareWeight = Double.compare(step1.getWeight(), step2.getWeight());
                if (compareWeight != 0) {
                    return compareWeight;
                }
                PatternVertex targetVertex1 =
                        e1.getGlogueEdge()
                                .getDstPattern()
                                .getVertexByOrder(step1.getTargetVertexOrder());
                PatternVertex targetVertex2 =
                        e2.getGlogueEdge()
                                .getDstPattern()
                                .getVertexByOrder(step2.getTargetVertexOrder());
                return targetVertex2.getId() - targetVertex1.getId();
            };
        }
    }

    private static class PruningStrategy {
        private final List<Predicate<PatternVertex>> predicates;

        public PruningStrategy(Pattern pattern) {
            predicates = Lists.newArrayList();
            // if pattern is disconnected after removing vertex v, prune it
            predicates.add(
                    (PatternVertex v) -> {
                        Pattern clone = new Pattern(pattern);
                        List connectedSets = clone.removeVertex(v);
                        return connectedSets.size() != 1;
                    });
            // constraint transformations if the pattern has optional vertices or edges
            List<PatternVertex> optionalVertices =
                    pattern.getVertexSet().stream()
                            .filter(k -> k.getElementDetails().isOptional())
                            .collect(Collectors.toList());
            if (!optionalVertices.isEmpty()) {
                // If there are optional vertices in the pattern, we should prioritize selecting
                // these vertices to perform rule transformations.
                // Vertices that do not belong to the optional set will be pruned.
                predicates.add((PatternVertex v) -> !optionalVertices.contains(v));
            } else {
                // If there are no optional vertices in the pattern, in which case the pattern only
                // consists of optional edges, we should first execute the part of the pattern that
                // does not contain optional edges.
                // After removing vertex v, if the subpattern contains optional edges, this case
                // will be pruned.
                predicates.add(
                        (PatternVertex v) -> {
                            Pattern clone = new Pattern(pattern);
                            clone.removeVertex(v);
                            return clone.getEdgeSet().stream()
                                    .anyMatch(k -> k.getElementDetails().isOptional());
                        });
            }
        }

        public boolean toPrune(PatternVertex target) {
            for (Predicate<PatternVertex> predicate : predicates) {
                if (predicate.test(target)) {
                    return true;
                }
            }
            return false;
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
        private boolean labelConstraintsEnabled;

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

        public ExtendIntersectRule.Config withLabelConstraintsEnabled(
                boolean labelConstraintsEnabled) {
            this.labelConstraintsEnabled = labelConstraintsEnabled;
            return this;
        }

        public boolean labelConstraintsEnabled() {
            return labelConstraintsEnabled;
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
