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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKeyMeta;
import com.alibaba.graphscope.common.ir.planner.rules.*;
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class PlannerGroup {
    private final RelOptPlanner relPlanner;
    private final RelOptPlanner matchPlanner;
    private final RelOptPlanner physicalPlanner;
    private final PlannerConfig config;
    private final RelBuilderFactory relBuilderFactory;

    public PlannerGroup(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
        this.config = config;
        this.relBuilderFactory = relBuilderFactory;
        this.relPlanner = createRelPlanner();
        this.matchPlanner = createMatchPlanner();
        this.physicalPlanner = createPhysicalPlanner();
    }

    public synchronized RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        if (config.isOn()) {
            // apply rules of 'FilterPushDown' before the match optimization
            relPlanner.setRoot(before);
            RelNode relOptimized = relPlanner.findBestExp();
            if (config.getOpt() == PlannerConfig.Opt.CBO) {
                relOptimized =
                        relOptimized.accept(
                                new GraphRelOptimizer.MatchOptimizer(ioProcessor, matchPlanner));
            }
            // apply rules of 'FieldTrim' after the match optimization
            if (config.getRules().contains(FieldTrimRule.class.getSimpleName())) {
                relOptimized = FieldTrimRule.trim(ioProcessor.getBuilder(), relOptimized);
            }
            physicalPlanner.setRoot(relOptimized);
            RelNode physicalOptimized = physicalPlanner.findBestExp();
            clear();
            return physicalOptimized;
        }
        return before;
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
                                }
                            });
            ruleConfigs.forEach(
                    k -> {
                        hepBuilder.addRuleInstance(
                                k.withRelBuilderFactory(relBuilderFactory).toRule());
                    });
        }
        return new GraphHepPlanner(hepBuilder.build());
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
                                                            config.getGlogueSize())
                                                    .withLabelConstraintsEnabled(
                                                            config.labelConstraintsEnabled());
                                } else if (k.equals(JoinDecompositionRule.class.getSimpleName())) {
                                    ruleConfig =
                                            JoinDecompositionRule.Config.DEFAULT
                                                    .withMinPatternSize(
                                                            config.getJoinMinPatternSize())
                                                    .withJoinQueueCapacity(
                                                            config.getJoinQueueCapacity())
                                                    .withJoinByEdgeEnabled(
                                                            config.isJoinByEdgeEnabled());
                                    ForeignKeyMeta foreignKeyMeta =
                                            config.getJoinByForeignKeyUri().isEmpty()
                                                    ? null
                                                    : new ForeignKeyMeta(
                                                            config.getJoinByForeignKeyUri());
                                    ((JoinDecompositionRule.Config) ruleConfig)
                                            .withForeignKeyMeta(foreignKeyMeta);
                                }
                                if (ruleConfig != null) {
                                    planner.addRule(
                                            ruleConfig
                                                    .withRelBuilderFactory(relBuilderFactory)
                                                    .toRule());
                                }
                            });
            return planner;
        }
        // todo: re-implement heuristic rules in ir core match
        return new GraphHepPlanner(HepProgram.builder().build());
    }

    private RelOptPlanner createPhysicalPlanner() {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (config.isOn()) {
            List<RelRule.Config> ruleConfigs = Lists.newArrayList();
            config.getRules()
                    .forEach(
                            k -> {
                                if (k.equals(ExpandGetVFusionRule.class.getSimpleName())) {
                                    ruleConfigs.add(
                                            ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config
                                                    .DEFAULT);
                                    ruleConfigs.add(
                                            ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config
                                                    .DEFAULT);
                                }
                            });
            ruleConfigs.forEach(
                    k -> {
                        hepBuilder.addRuleInstance(
                                k.withRelBuilderFactory(relBuilderFactory).toRule());
                    });
        }
        return new GraphHepPlanner(hepBuilder.build());
    }

    public synchronized void clear() {
        List<RelOptRule> logicalRBORules = this.relPlanner.getRules();
        this.relPlanner.clear();
        for (RelOptRule rule : logicalRBORules) {
            this.relPlanner.addRule(rule);
        }
        List<RelOptRule> logicalCBORules = this.matchPlanner.getRules();
        this.matchPlanner.clear();
        for (RelOptRule rule : logicalCBORules) {
            this.matchPlanner.addRule(rule);
        }
        List<RelOptRule> physicalRBORules = this.physicalPlanner.getRules();
        this.physicalPlanner.clear();
        for (RelOptRule rule : physicalRBORules) {
            this.physicalPlanner.addRule(rule);
        }
    }

    public RelOptPlanner getMatchPlanner() {
        return this.matchPlanner;
    }
}
