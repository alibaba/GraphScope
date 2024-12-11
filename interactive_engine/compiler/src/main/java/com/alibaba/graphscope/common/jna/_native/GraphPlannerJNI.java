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

package com.alibaba.graphscope.common.jna._native;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;

public class GraphPlannerJNI {
    /**
     * Provide a java-side implementation to compile the query in string to a physical plan
     * @param config
     * @param query
     * @return JNIPlan has two fields: physicalBytes and resultSchemaYaml,
     * physicalBytes can be decoded to {@code PhysicalPlan} in c++ side by standard PB serialization,
     * resultSchemaYaml defines the result specification of the query in yaml format
     * @throws Exception
     */
    public static JNIPlan compilePlan(String config, String query) throws Exception {
        GraphPlanner.Summary summary = GraphPlanner.generatePlan(config, query);
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
        return new JNIPlan(physicalPlan.getContent(), new String(metaStream.toByteArray()));
    }
}
