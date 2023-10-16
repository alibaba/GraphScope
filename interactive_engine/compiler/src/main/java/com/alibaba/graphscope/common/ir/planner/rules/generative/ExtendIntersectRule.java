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

package com.alibaba.graphscope.common.ir.planner.rules.generative;

import com.alibaba.graphscope.common.ir.meta.glogue.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
            throw new UnsupportedOperationException(
                    "pattern graph with size="
                            + patternSize
                            + " > maxSize="
                            + maxPatternSize
                            + " is unsupported yet");
        }
        Collections.sort(
                edges,
                (GraphExtendIntersect i1, GraphExtendIntersect i2) ->
                        mq.getNonCumulativeCost(i1).isLe(mq.getNonCumulativeCost(i2)) ? -1 : 1);
        return edges;
    }

    public static class Config implements RelRule.Config {
        public static ExtendIntersectRule.Config DEFAULT =
                new ExtendIntersectRule.Config()
                        .withOperandSupplier(b0 -> b0.operand(GraphPattern.class).anyInputs())
                        .withMaxPatternSizeInGlogue(3);

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
