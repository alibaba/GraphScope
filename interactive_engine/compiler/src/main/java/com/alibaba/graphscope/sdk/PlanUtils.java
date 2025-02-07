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

package com.alibaba.graphscope.sdk;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.IrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphStatistics;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaInputStream;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpec;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.proto.frontend.Code;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PlanUtils {
    private static final Logger logger = LoggerFactory.getLogger(PlanUtils.class);
    /**
     * Provide a java-side implementation to compile the query in string to a physical plan
     * @param configPath
     * @param query
     *
     * @return JNIPlan has two fields: physicalBytes and resultSchemaYaml,
     * physicalBytes can be decoded to {@code PhysicalPlan} in c++ side by standard PB serialization,
     * resultSchemaYaml defines the result specification of the query in yaml format
     * @throws Exception
     */
    public static GraphPlan compilePlan(
            String configPath, String query, String schemaYaml, String statsJson) {
        try {
            long startTime = System.currentTimeMillis();
            Configs configs = Configs.Factory.create(configPath);
            GraphPlanner graphPlanner = GraphPlanerInstance.getInstance(configs);
            IrMetaReader reader = new StringMetaReader(schemaYaml, statsJson, configs);
            IrMetaFetcher metaFetcher =
                    new StaticIrMetaFetcher(reader, graphPlanner.getOptimizer().getGlogueHolder());
            GraphPlanner.PlannerInstance plannerInstance =
                    graphPlanner.instance(query, metaFetcher.fetch().get());
            GraphPlanner.Summary summary = plannerInstance.plan();
            LogicalPlan logicalPlan = summary.getLogicalPlan();
            PhysicalPlan<byte[]> physicalPlan = summary.getPhysicalPlan();
            StoredProcedureMeta procedureMeta =
                    new StoredProcedureMeta(
                            new Configs(ImmutableMap.of()),
                            query,
                            logicalPlan.getOutputType(),
                            logicalPlan.getDynamicParams());
            ByteArrayOutputStream metaStream = new ByteArrayOutputStream();
            StoredProcedureMeta.Serializer.perform(procedureMeta, metaStream, false);
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("compile plan cost: {} ms", elapsedTime);
            // hack ways to set error code as 'TYPE_INFERENCE_FAILED' when the results are empty
            if (physicalPlan.getContent() == null) {
                return new GraphPlan(
                        Code.TYPE_INFERENCE_FAILED,
                        "The compiled plan returns empty results",
                        null,
                        null);
            }
            return new GraphPlan(
                    Code.OK, null, physicalPlan.getContent(), new String(metaStream.toByteArray()));
        } catch (Throwable t) {
            if (t instanceof FrontendException) {
                return new GraphPlan(
                        ((FrontendException) t).getErrorCode(), t.getMessage(), null, null);
            }
            return new GraphPlan(Code.UNRECOGNIZED, t.getMessage(), null, null);
        }
    }

    static class StringMetaReader implements IrMetaReader {
        private final String schemaYaml;
        private final String statsJson;
        private final Configs configs;

        public StringMetaReader(String schemaYaml, String statsJson, Configs configs) {
            this.schemaYaml = schemaYaml;
            this.statsJson = statsJson;
            this.configs = configs;
        }

        @Override
        public IrMeta readMeta() throws IOException {
            IrGraphSchema graphSchema =
                    new IrGraphSchema(
                            configs,
                            new SchemaInputStream(
                                    new ByteArrayInputStream(
                                            schemaYaml.getBytes(StandardCharsets.UTF_8)),
                                    SchemaSpec.Type.FLEX_IN_YAML));
            return new IrMeta(
                    graphSchema,
                    new GraphStoredProcedures(
                            new ByteArrayInputStream(schemaYaml.getBytes(StandardCharsets.UTF_8)),
                            this));
        }

        @Override
        public GraphStatistics readStats(GraphId graphId) throws IOException {
            return new IrGraphStatistics(
                    new ByteArrayInputStream(statsJson.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public boolean syncStatsEnabled(GraphId graphId) throws IOException {
            return false;
        }
    }
}
