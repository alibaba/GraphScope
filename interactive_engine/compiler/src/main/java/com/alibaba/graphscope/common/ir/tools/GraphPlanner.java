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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ProcedurePhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.schema.StatisticSchema;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class GraphPlanner {
    private final Configs graphConfig;
    private final PlannerConfig plannerConfig;
    private final RelOptPlanner optPlanner;
    private final RexBuilder rexBuilder;
    private final AtomicLong idGenerator;

    public GraphPlanner(Configs graphConfig) {
        this.graphConfig = graphConfig;
        this.plannerConfig = PlannerConfig.create(this.graphConfig);
        this.optPlanner = createRelOptPlanner(this.plannerConfig);
        this.rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
        this.idGenerator = new AtomicLong(0l);
    }

    public PlannerInstance instance(ParseTree parsedQuery, IrMeta irMeta) {
        long id = idGenerator.getAndIncrement();
        String name = "ir_plan_" + id;
        GraphOptCluster optCluster = GraphOptCluster.create(this.optPlanner, this.rexBuilder);
        return new PlannerInstance(id, name, parsedQuery, optCluster, irMeta);
    }

    public class PlannerInstance {
        private final long id;
        private final String name;
        private final ParseTree parsedQuery;
        private final GraphOptCluster optCluster;
        private final IrMeta irMeta;

        public PlannerInstance(
                long id,
                String name,
                ParseTree parsedQuery,
                GraphOptCluster optCluster,
                IrMeta irMeta) {
            this.id = id;
            this.name = name;
            this.parsedQuery = parsedQuery;
            this.optCluster = optCluster;
            this.irMeta = irMeta;
        }

        public Summary plan() {
            // build logical plan from parsed query
            StatisticSchema schema = irMeta.getSchema();
            GraphBuilder graphBuilder =
                    GraphBuilder.create(
                            null, this.optCluster, new GraphOptSchema(this.optCluster, schema));
            LogicalPlan logicalPlan =
                    new LogicalPlanVisitor(graphBuilder, this.irMeta).visit(this.parsedQuery);
            // apply optimizations
            if (plannerConfig.isOn()
                    && logicalPlan.getRegularQuery() != null
                    && !logicalPlan.isReturnEmpty()) {
                RelNode regularQuery = logicalPlan.getRegularQuery();
                RelOptPlanner planner = this.optCluster.getPlanner();
                planner.setRoot(regularQuery);
                logicalPlan = new LogicalPlan(planner.findBestExp(), logicalPlan.isReturnEmpty());
            }
            // build physical plan from logical plan
            PhysicalBuilder physicalBuilder;
            if (logicalPlan.isReturnEmpty()) {
                physicalBuilder = PhysicalBuilder.createEmpty(logicalPlan);
            } else if (logicalPlan.getRegularQuery() != null) {
                physicalBuilder = new FfiPhysicalBuilder(graphConfig, irMeta, logicalPlan);
            } else {
                physicalBuilder = new ProcedurePhysicalBuilder(logicalPlan);
            }
            return new Summary(this.id, this.name, logicalPlan, physicalBuilder);
        }
    }

    public static class Summary {
        private final long id;
        private final String name;
        private final LogicalPlan logicalPlan;
        private final PhysicalBuilder physicalBuilder;

        public Summary(
                long id, String name, LogicalPlan logicalPlan, PhysicalBuilder physicalBuilder) {
            this.id = id;
            this.name = name;
            this.logicalPlan = Objects.requireNonNull(logicalPlan);
            this.physicalBuilder = Objects.requireNonNull(physicalBuilder);
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public @Nullable PhysicalBuilder getPhysicalBuilder() {
            return physicalBuilder;
        }
    }

    private RelOptPlanner createRelOptPlanner(PlannerConfig plannerConfig) {
        if (plannerConfig.isOn()) {
            PlannerConfig.Opt opt = plannerConfig.getOpt();
            switch (opt) {
                case RBO:
                    HepProgramBuilder hepBuilder = HepProgram.builder();
                    plannerConfig
                            .getRules()
                            .forEach(
                                    k -> {
                                        if (k.equals(FilterMatchRule.class.getSimpleName())) {
                                            hepBuilder.addRuleInstance(
                                                    FilterMatchRule.Config.DEFAULT.toRule());
                                        } else {
                                            // todo: add more rules
                                        }
                                    });
                    return new HepPlanner(hepBuilder.build());
                case CBO:
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
