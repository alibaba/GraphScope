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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler.GraphMetadataHandlerProvider;
import com.alibaba.graphscope.common.ir.planner.rules.ExtendIntersectRule;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.planner.rules.NotMatchToAntiJoinRule;
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleX;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Optimize graph relational tree which consists of match and other relational operators
 */
public class GraphRelOptimizer {
    private final PlannerConfig config;
    private final RelOptPlanner relPlanner;
    private final RelOptPlanner matchPlanner;

    public GraphRelOptimizer(PlannerConfig config) {
        this.config = Objects.requireNonNull(config);
        this.relPlanner = createRelPlanner();
        this.matchPlanner = createMatchPlanner();
    }

    public RelOptPlanner getMatchPlanner() {
        return matchPlanner;
    }

    public @Nullable RelMetadataQuery createMetaDataQuery() {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            GlogueSchema g = config.getGlogueSchema();
            Glogue gl = new Glogue().create(g, config.getGlogueSize());
            GlogueQuery gq = new GlogueQuery(gl, g);
            return new GraphRelMetadataQuery(
                    new GraphMetadataHandlerProvider(this.matchPlanner, gq));
        }
        return null;
    }

    public RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        if (config.isOn()) {
            // apply rules of 'FilterPushDown' before the match optimization
            relPlanner.setRoot(before);
            RelNode relOptimized = relPlanner.findBestExp();
            return relOptimized.accept(new MatchOptimizer(ioProcessor));
        }
        return before;
    }

    private class MatchOptimizer extends GraphRelShuttleX {
        private final GraphIOProcessor ioProcessor;

        public MatchOptimizer(GraphIOProcessor ioProcessor) {
            this.ioProcessor = ioProcessor;
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(matchPlanner.findBestExp());
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(matchPlanner.findBestExp());
        }
    }

    private RelOptPlanner createRelPlanner() {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (config.isOn()) {
            List<RelRule.Config> ruleConfigs = Lists.newArrayList();
            config.getRules()
                    .forEach(
                            k -> {
                                if (k.equals(
                                        FilterJoinRule.FilterIntoJoinRule.class.getSimpleName())) {
                                    ruleConfigs.add(CoreRules.FILTER_INTO_JOIN.config);
                                } else if (k.equals(FilterMatchRule.class.getSimpleName())) {
                                    ruleConfigs.add(FilterMatchRule.Config.DEFAULT);
                                } else if (k.equals(NotMatchToAntiJoinRule.class.getSimpleName())) {
                                    ruleConfigs.add(NotMatchToAntiJoinRule.Config.DEFAULT);
                                }
                            });
            ruleConfigs.forEach(
                    k -> {
                        hepBuilder.addRuleInstance(
                                k.withRelBuilderFactory(GraphPlanner.relBuilderFactory).toRule());
                    });
        }
        return new HepPlanner(hepBuilder.build());
    }

    private RelOptPlanner createMatchPlanner() {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            VolcanoPlanner planner = new VolcanoPlannerX();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            planner.setTopDownOpt(true);
            planner.setNoneConventionHasInfiniteCost(false);
            config.getRules()
                    .forEach(
                            k -> {
                                RelRule.Config ruleConfig = null;
                                if (k.equals(ExtendIntersectRule.class.getSimpleName())) {
                                    ruleConfig =
                                            ExtendIntersectRule.Config.DEFAULT
                                                    .withMaxPatternSizeInGlogue(
                                                            config.getGlogueSize());
                                } else {
                                    // todo: add JoinRule
                                }
                                if (ruleConfig != null) {
                                    planner.addRule(
                                            ruleConfig
                                                    .withRelBuilderFactory(
                                                            GraphPlanner.relBuilderFactory)
                                                    .toRule());
                                }
                            });
            return planner;
        }
        // todo: re-implement heuristic rules in ir core match
        return new HepPlanner(HepProgram.builder().build());
    }
}
