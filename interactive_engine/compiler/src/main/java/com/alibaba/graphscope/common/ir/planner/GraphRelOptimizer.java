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

import org.apache.calcite.plan.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Optimize graph relational tree which consists of match and other relational operators
 */
public class GraphRelOptimizer {
    private static final Logger logger = LoggerFactory.getLogger(GraphRelOptimizer.class);
    private final PlannerConfig config;
    private final RelBuilderFactory relBuilderFactory;
    private final GlogueHolder glogueHolder;
    private final PlannerGroupManager plannerGroupManager;

    public GraphRelOptimizer(Configs graphConfig) {
        this.config = new PlannerConfig(graphConfig);
        this.relBuilderFactory = new GraphBuilderFactory(graphConfig);
        this.glogueHolder = new GlogueHolder(graphConfig);
        this.plannerGroupManager = new PlannerGroupManager();
    }

    public GlogueHolder getGlogueHolder() {
        return glogueHolder;
    }

    public RelOptPlanner getMatchPlanner() {
        PlannerGroup currentGroup = this.plannerGroupManager.getCurrentGroup();
        return currentGroup.matchPlanner;
    }

    public RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        PlannerGroup currentGroup = this.plannerGroupManager.getCurrentGroup();
        return currentGroup.optimize(before, ioProcessor);
    }

    public @Nullable RelMetadataQuery createMetaDataQuery(IrMeta irMeta) {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            GlogueQuery gq = this.glogueHolder.getGlogue();
            Preconditions.checkArgument(gq != null, "glogue is not ready");
            return new GraphRelMetadataQuery(
                    new GraphMetadataHandlerProvider(getMatchPlanner(), gq, this.config));
        }
        return null;
    }

    private class MatchOptimizer extends GraphShuttle {
        private final GraphIOProcessor ioProcessor;
        private final RelOptPlanner matchPlanner;

        public MatchOptimizer(GraphIOProcessor ioProcessor, RelOptPlanner matchPlanner) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
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

    private class PlannerGroup {
        private final RelOptPlanner relPlanner;
        private final RelOptPlanner matchPlanner;
        private final RelOptPlanner physicalPlanner;

        public PlannerGroup() {
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
                            relOptimized.accept(new MatchOptimizer(ioProcessor, matchPlanner));
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
                                            FilterJoinRule.FilterIntoJoinRule.class
                                                    .getSimpleName())) {
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
                                    } else if (k.equals(
                                            JoinDecompositionRule.class.getSimpleName())) {
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
                                                ExpandGetVFusionRule.BasicExpandGetVFusionRule
                                                        .Config.DEFAULT);
                                        ruleConfigs.add(
                                                ExpandGetVFusionRule.PathBaseExpandGetVFusionRule
                                                        .Config.DEFAULT);
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

        private synchronized void clear() {
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
    }

    private class PlannerGroupManager {
        private final List<PlannerGroup> plannerGroups;
        private final ScheduledExecutorService clearScheduler;

        public PlannerGroupManager() {
            Preconditions.checkArgument(
                    config.getPlannerGroupSize() > 0,
                    "planner group size should be greater than 0");
            this.plannerGroups = new ArrayList(config.getPlannerGroupSize());
            for (int i = 0; i < config.getPlannerGroupSize(); ++i) {
                this.plannerGroups.add(new PlannerGroup());
            }
            this.clearScheduler = new ScheduledThreadPoolExecutor(1);
            int clearInterval = config.getPlannerGroupClearIntervalMinutes();
            this.clearScheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            long freeMemBytes = Runtime.getRuntime().freeMemory();
                            long totalMemBytes = Runtime.getRuntime().totalMemory();
                            Preconditions.checkArgument(
                                    totalMemBytes > 0, "total memory should be greater than 0");
                            if (freeMemBytes / (double) totalMemBytes < 0.2d) {
                                logger.warn(
                                        "start to clear planner groups. There are no enough memory"
                                                + " in JVM, with free memory: {}, total memory: {}",
                                        freeMemBytes,
                                        totalMemBytes);
                                plannerGroups.forEach(PlannerGroup::clear);
                            }
                        } catch (Throwable t) {
                            logger.error("failed to clear planner group.", t);
                        }
                    },
                    clearInterval,
                    clearInterval,
                    TimeUnit.MINUTES);
        }

        public PlannerGroup getCurrentGroup() {
            Preconditions.checkArgument(
                    !plannerGroups.isEmpty(), "planner groups should not be empty");
            int groupId = (int) Thread.currentThread().getId() % plannerGroups.size();
            return plannerGroups.get(groupId);
        }
    }
}
