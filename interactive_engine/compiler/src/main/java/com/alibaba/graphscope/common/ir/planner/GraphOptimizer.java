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
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleX;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class GraphOptimizer {
    private final PlannerConfig config;
    private final RelOptPlanner graphOptPlanner;

    public GraphOptimizer(PlannerConfig config) {
        this.config = Objects.requireNonNull(config);
        this.graphOptPlanner = createGraphOptPlanner();
    }

    public RelOptPlanner getGraphOptPlanner() {
        return graphOptPlanner;
    }

    public @Nullable RelMetadataQuery createMetaDataQuery() {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
            Glogue gl = new Glogue().create(g, config.getGlogueSize());
            GlogueQuery gq = new GlogueQuery(gl, g);
            return new GraphRelMetadataQuery(
                    new GraphMetadataHandlerProvider(this.graphOptPlanner, gq));
        }
        return null;
    }

    public RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        return before.accept(new Convertor(ioProcessor));
    }

    private class Convertor extends GraphRelShuttleX {
        private final GraphIOProcessor ioProcessor;

        public Convertor(GraphIOProcessor ioProcessor) {
            this.ioProcessor = ioProcessor;
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            graphOptPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(graphOptPlanner.findBestExp());
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            graphOptPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(graphOptPlanner.findBestExp());
        }
    }

    private RelOptPlanner createGraphOptPlanner() {
        if (config.isOn()) {
            PlannerConfig.Opt opt = config.getOpt();
            switch (opt) {
                case CBO:
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
                case RBO:
                default:
                    throw new UnsupportedOperationException(
                            "planner type " + opt.name() + " is unsupported yet");
            }
        } else {
            // return HepPlanner with empty rules if optimization is turned off
            return new HepPlanner(HepProgram.builder().build());
        }
    }
}
