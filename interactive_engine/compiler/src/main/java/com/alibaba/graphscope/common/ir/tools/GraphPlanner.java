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

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.planner.rules.NotMatchToAntiJoinRule;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ProcedurePhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A unified structure to build {@link PlannerInstance} which can further build logical and physical plan from an antlr tree
 */
public class GraphPlanner {
    private static final Logger logger = LoggerFactory.getLogger(GraphPlanner.class);
    private final Configs graphConfig;
    private final PlannerConfig plannerConfig;
    private final RelOptPlanner optPlanner;
    private final RexBuilder rexBuilder;
    private final AtomicLong idGenerator;
    private static final RelBuilderFactory relBuilderFactory =
            (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                    GraphBuilder.create(null, (GraphOptCluster) cluster, schema);

    public GraphPlanner(Configs graphConfig) {
        this.graphConfig = graphConfig;
        this.plannerConfig = PlannerConfig.create(this.graphConfig);
        logger.debug("planner config: " + this.plannerConfig);
        this.optPlanner = createRelOptPlanner(this.plannerConfig);
        this.rexBuilder = new GraphRexBuilder(new GraphTypeFactoryImpl());
        this.idGenerator = new AtomicLong(FrontendConfig.FRONTEND_SERVER_ID.get(graphConfig));
    }

    public PlannerInstance instance(ParseTree parsedQuery, IrMeta irMeta) {
        long id = generateInstanceId();
        String name = "ir_plan_" + id;
        GraphOptCluster optCluster = GraphOptCluster.create(this.optPlanner, this.rexBuilder);
        return new PlannerInstance(id, name, parsedQuery, optCluster, irMeta);
    }

    public long generateInstanceId() {
        long delta = FrontendConfig.FRONTEND_SERVER_NUM.get(graphConfig);
        return idGenerator.getAndAdd(delta);
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
            IrGraphSchema schema = irMeta.getSchema();
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
                logicalPlan =
                        new LogicalPlan(planner.findBestExp(), logicalPlan.getDynamicParams());
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
                    List<RelRule.Config> ruleConfigs = Lists.newArrayList();
                    plannerConfig
                            .getRules()
                            .forEach(
                                    k -> {
                                        if (k.equals(
                                                FilterJoinRule.FilterIntoJoinRule.class
                                                        .getSimpleName())) {
                                            ruleConfigs.add(CoreRules.FILTER_INTO_JOIN.config);
                                        } else if (k.equals(
                                                FilterMatchRule.class.getSimpleName())) {
                                            ruleConfigs.add(FilterMatchRule.Config.DEFAULT);
                                        } else if (k.equals(
                                                NotMatchToAntiJoinRule.class.getSimpleName())) {
                                            ruleConfigs.add(NotMatchToAntiJoinRule.Config.DEFAULT);
                                        } else {
                                            // todo: add more rule configs
                                        }
                                    });
                    HepProgramBuilder hepBuilder = HepProgram.builder();
                    ruleConfigs.forEach(
                            k -> {
                                hepBuilder.addRuleInstance(
                                        k.withRelBuilderFactory(relBuilderFactory).toRule());
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

    private static Configs createExtraConfigs(@Nullable String keyValues) {
        Map<String, String> keyValueMap = Maps.newHashMap();
        if (!StringUtils.isEmpty(keyValues)) {
            String[] pairs = keyValues.split(",");
            for (String pair : pairs) {
                String[] kv = pair.trim().split(":");
                Preconditions.checkArgument(
                        kv.length == 2, "invalid key value pair: " + pair + " in " + keyValues);
                keyValueMap.put(kv[0], kv[1]);
            }
        }
        return new Configs(keyValueMap);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4
                || args[0].isEmpty()
                || args[1].isEmpty()
                || args[2].isEmpty()
                || args[3].isEmpty()) {
            throw new IllegalArgumentException(
                    "usage: GraphPlanner '<path_to_config_file>' '<path_to_query_file>' "
                            + " '<path_to_physical_output_file>' '<path_to_procedure_file>'"
                            + " 'optional <extra_key_value_config_pairs>'");
        }
        Configs configs = Configs.Factory.create(args[0]);
        ExperimentalMetaFetcher metaFetcher =
                new ExperimentalMetaFetcher(new LocalMetaDataReader(configs));
        String query = FileUtils.readFileToString(new File(args[1]), StandardCharsets.UTF_8);
        GraphPlanner planner = new GraphPlanner(configs);
        Antlr4Parser cypherParser = new CypherAntlr4Parser();
        PlannerInstance instance =
                planner.instance(cypherParser.parse(query), metaFetcher.fetch().get());
        Summary summary = instance.plan();
        // write physical plan to file
        try (PhysicalBuilder<byte[]> physicalBuilder = summary.getPhysicalBuilder()) {
            FileUtils.writeByteArrayToFile(new File(args[2]), physicalBuilder.build());
        }
        // write stored procedure meta to file
        LogicalPlan logicalPlan = summary.getLogicalPlan();
        Configs extraConfigs = createExtraConfigs(args.length > 4 ? args[4] : null);
        StoredProcedureMeta procedureMeta =
                new StoredProcedureMeta(
                        extraConfigs, logicalPlan.getOutputType(), logicalPlan.getDynamicParams());
        StoredProcedureMeta.Serializer.perform(procedureMeta, new FileOutputStream(args[3]));
    }
}
