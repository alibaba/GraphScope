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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler.GraphMetadataHandlerProvider;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKeyMeta;
import com.alibaba.graphscope.common.ir.planner.rules.*;
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.tools.GraphBuilderFactory;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Optimize graph relational tree which consists of match and other relational operators
 */
public class GraphRelOptimizer {
    private final Configs graphConfig;
    private final PlannerConfig config;
    private final RelOptPlanner relPlanner;
    private final RelOptPlanner matchPlanner;
    private final RelOptPlanner physicalPlanner;
    private final RelBuilderFactory relBuilderFactory;
    private final GlogueHolder glogueHolder;

    public GraphRelOptimizer(Configs graphConfig) {
        this.graphConfig = graphConfig;
        this.config = new PlannerConfig(graphConfig);
        this.relBuilderFactory = new GraphBuilderFactory(graphConfig);
        this.relPlanner = createRelPlanner();
        this.matchPlanner = createMatchPlanner();
        this.physicalPlanner = createPhysicalPlanner();
        this.glogueHolder = new GlogueHolder(graphConfig);
    }

    public GlogueHolder getGlogueHolder() {
        return glogueHolder;
    }

    public RelOptPlanner getMatchPlanner() {
        return matchPlanner;
    }

    public RelOptPlanner getPhysicalPlanner() {
        return physicalPlanner;
    }

    public RelOptPlanner getRelPlanner() {
        return relPlanner;
    }

    public @Nullable RelMetadataQuery createMetaDataQuery(IrMeta irMeta) {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            GlogueQuery gq = this.glogueHolder.getGlogue();
            Preconditions.checkArgument(gq != null, "glogue is not ready");
            return new GraphRelMetadataQuery(
                    new GraphMetadataHandlerProvider(this.matchPlanner, gq, this.config));
        }
        return null;
    }

    public RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        if (config.isOn()) {
            // apply rules of 'FilterPushDown' before the match optimization
            relPlanner.setRoot(before);
            RelNode relOptimized = relPlanner.findBestExp();
            if (config.getOpt() == PlannerConfig.Opt.CBO) {
                relOptimized = relOptimized.accept(new MatchOptimizer(ioProcessor));
            }
            // apply rules of 'FieldTrim' after the match optimization
            if (config.getRules().contains(FieldTrimRule.class.getSimpleName())) {
                relOptimized = FieldTrimRule.trim(ioProcessor.getBuilder(), relOptimized);
            }
            physicalPlanner.setRoot(relOptimized);
            RelNode physicalOptimized = physicalPlanner.findBestExp();
            return physicalOptimized;
        }
        return before;
    }

    private class MatchOptimizer extends GraphShuttle {
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

        @Override
        public RelNode visit(LogicalJoin join) {
            List<RelNode> matchList = Lists.newArrayList();
            List<RelNode> filterList = Lists.newArrayList();
            if (!decomposeJoin(join, matchList, filterList)) {
                return super.visit(join);
            } else {
                matchPlanner.setRoot(ioProcessor.processInput(matchList));
                RelNode match = ioProcessor.processOutput(matchPlanner.findBestExp());
                for (RelNode filter : filterList) {
                    match = filter.copy(filter.getTraitSet(), ImmutableList.of(match));
                }
                return match;
            }
        }

        private boolean decomposeJoin(
                LogicalJoin join, List<RelNode> matchList, List<RelNode> filterList) {
            AtomicBoolean decomposable = new AtomicBoolean(true);
            RelVisitor visitor =
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            if (node instanceof LogicalJoin) {
                                JoinRelType joinType = ((LogicalJoin) node).getJoinType();
                                if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
                                    decomposable.set(false);
                                    return;
                                }
                                visit(((LogicalJoin) node).getLeft(), 0, node);
                                if (!decomposable.get()) {
                                    return;
                                }
                                int leftMatchSize = matchList.size();
                                visit(((LogicalJoin) node).getRight(), 1, node);
                                if (!decomposable.get()) {
                                    return;
                                }
                                if (joinType == JoinRelType.LEFT) {
                                    for (int i = leftMatchSize; i < matchList.size(); i++) {
                                        if (matchList.get(i) instanceof GraphLogicalSingleMatch) {
                                            GraphLogicalSingleMatch singleMatch =
                                                    (GraphLogicalSingleMatch) matchList.get(i);
                                            matchList.set(
                                                    i,
                                                    GraphLogicalSingleMatch.create(
                                                            (GraphOptCluster)
                                                                    singleMatch.getCluster(),
                                                            ImmutableList.of(),
                                                            singleMatch.getInput(),
                                                            singleMatch.getSentence(),
                                                            GraphOpt.Match.OPTIONAL));
                                        }
                                    }
                                }
                            } else if (node instanceof AbstractLogicalMatch) {
                                matchList.add(node);
                            } else if (node instanceof GraphLogicalSource) {
                                matchList.add(
                                        GraphLogicalSingleMatch.create(
                                                (GraphOptCluster) node.getCluster(),
                                                ImmutableList.of(),
                                                null,
                                                node,
                                                GraphOpt.Match.INNER));
                            } else if (node instanceof Filter) {
                                filterList.add(node);
                                visit(node.getInput(0), 0, node);
                            } else {
                                decomposable.set(false);
                            }
                        }
                    };
            visitor.go(join);
            return decomposable.get();
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
                                }
                            });
            ruleConfigs.forEach(
                    k -> {
                        hepBuilder.addRuleInstance(
                                k.withRelBuilderFactory(relBuilderFactory).toRule());
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
                                    if (!config.getJoinByForeignKeyUri().isEmpty()) {
                                        ((JoinDecompositionRule.Config) ruleConfig)
                                                .withForeignKeyMeta(
                                                        new ForeignKeyMeta(
                                                                config.getJoinByForeignKeyUri()));
                                    }
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
        return new HepPlanner(HepProgram.builder().build());
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
}
