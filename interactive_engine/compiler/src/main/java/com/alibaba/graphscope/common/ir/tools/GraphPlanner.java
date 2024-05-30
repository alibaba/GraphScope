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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.ProcedurePhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A unified structure to build {@link PlannerInstance} which can further build logical and physical plan from an antlr tree
 */
public class GraphPlanner {
    private static final Logger logger = LoggerFactory.getLogger(GraphPlanner.class);
    private final Configs graphConfig;
    private final GraphRelOptimizer optimizer;
    private final RexBuilder rexBuilder;
    private final LogicalPlanFactory logicalPlanFactory;

    public static final Function<Configs, RexBuilder> rexBuilderFactory =
            (Configs configs) -> new GraphRexBuilder(new GraphTypeFactoryImpl(configs));

    public GraphPlanner(
            Configs graphConfig,
            LogicalPlanFactory logicalPlanFactory,
            GraphRelOptimizer optimizer) {
        this.graphConfig = graphConfig;
        this.optimizer = optimizer;
        this.logicalPlanFactory = logicalPlanFactory;
        this.rexBuilder = rexBuilderFactory.apply(graphConfig);
    }

    public PlannerInstance instance(String query, IrMeta irMeta) {
        GraphOptCluster optCluster =
                GraphOptCluster.create(this.optimizer.getMatchPlanner(), this.rexBuilder);
        RelMetadataQuery mq = optimizer.createMetaDataQuery(irMeta);
        if (mq != null) {
            optCluster.setMetadataQuerySupplier(() -> mq);
        }
        return new PlannerInstance(query, optCluster, irMeta);
    }

    public class PlannerInstance {
        private final String query;
        private final GraphOptCluster optCluster;
        private final IrMeta irMeta;

        public PlannerInstance(String query, GraphOptCluster optCluster, IrMeta irMeta) {
            this.query = query;
            this.optCluster = optCluster;
            this.irMeta = irMeta;
        }

        public Summary plan() {
            LogicalPlan logicalPlan = planLogical();
            return new Summary(logicalPlan, planPhysical(logicalPlan));
        }

        public LogicalPlan planLogical() {
            // build logical plan from parsed query
            IrGraphSchema schema = irMeta.getSchema();
            GraphBuilder graphBuilder =
                    GraphBuilder.create(
                            graphConfig,
                            this.optCluster,
                            new GraphOptSchema(this.optCluster, schema));

            LogicalPlan logicalPlan = logicalPlanFactory.create(graphBuilder, irMeta, query);

            // apply optimizations
            if (logicalPlan.getRegularQuery() != null && !logicalPlan.isReturnEmpty()) {
                RelNode before = logicalPlan.getRegularQuery();
                RelNode after =
                        optimizer.optimize(before, new GraphIOProcessor(graphBuilder, irMeta));
                if (after != before) {
                    logicalPlan = new LogicalPlan(after, logicalPlan.getDynamicParams());
                }
            }
            return logicalPlan;
        }

        public PhysicalPlan planPhysical(LogicalPlan logicalPlan) {
            // build physical plan from logical plan
            if (logicalPlan.isReturnEmpty()) {
                return PhysicalPlan.createEmpty();
            } else if (logicalPlan.getRegularQuery() != null) {
                String physicalOpt = FrontendConfig.PHYSICAL_OPT_CONFIG.get(graphConfig);
                if ("proto".equals(physicalOpt.toLowerCase())) {
                    logger.debug("physical type is proto");
                    try (GraphRelProtoPhysicalBuilder physicalBuilder =
                            new GraphRelProtoPhysicalBuilder(graphConfig, irMeta, logicalPlan)) {
                        return physicalBuilder.build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    logger.debug("physical type is ffi");
                    try (PhysicalBuilder physicalBuilder =
                            new FfiPhysicalBuilder(graphConfig, irMeta, logicalPlan)) {
                        return physicalBuilder.build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                return new ProcedurePhysicalBuilder(logicalPlan).build();
            }
        }
    }

    public static class Summary {
        private final LogicalPlan logicalPlan;
        private final PhysicalPlan physicalPlan;

        public Summary(LogicalPlan logicalPlan, PhysicalPlan physicalPlan) {
            this.logicalPlan = Objects.requireNonNull(logicalPlan);
            this.physicalPlan = Objects.requireNonNull(physicalPlan);
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public PhysicalPlan getPhysicalPlan() {
            return physicalPlan;
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
        GraphRelOptimizer optimizer = new GraphRelOptimizer(configs);
        StaticIrMetaFetcher metaFetcher =
                new StaticIrMetaFetcher(
                        new LocalIrMetaReader(configs), optimizer.getGlogueHolder());
        String query = FileUtils.readFileToString(new File(args[1]), StandardCharsets.UTF_8);
        GraphPlanner planner =
                new GraphPlanner(
                        configs,
                        (GraphBuilder builder, IrMeta irMeta, String q) ->
                                new LogicalPlanVisitor(builder, irMeta)
                                        .visit(new CypherAntlr4Parser().parse(q)),
                        optimizer);
        PlannerInstance instance = planner.instance(query, metaFetcher.fetch().get());
        Summary summary = instance.plan();
        // write physical plan to file
        PhysicalPlan<byte[]> physicalPlan = summary.physicalPlan;
        FileUtils.writeByteArrayToFile(new File(args[2]), physicalPlan.getContent());
        // write stored procedure meta to file
        LogicalPlan logicalPlan = summary.getLogicalPlan();
        Configs extraConfigs = createExtraConfigs(args.length > 4 ? args[4] : null);
        StoredProcedureMeta procedureMeta =
                new StoredProcedureMeta(
                        extraConfigs,
                        query,
                        logicalPlan.getOutputType(),
                        logicalPlan.getDynamicParams());
        StoredProcedureMeta.Serializer.perform(procedureMeta, new FileOutputStream(args[3]));
    }
}
