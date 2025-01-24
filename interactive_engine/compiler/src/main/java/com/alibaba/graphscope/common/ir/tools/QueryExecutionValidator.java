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

import com.alibaba.graphscope.common.config.Config;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.proto.frontend.Code;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class QueryExecutionValidator {
    public static final int SYSTEM_MAX_ITERATIONS = 15;

    private static final Config<Integer> CONFIG_MAX_ITERATIONS =
            Config.intConfig("query.execution.max.iterations", 15);

    private final int maxIterations;

    public QueryExecutionValidator(Configs configs) {
        int maxIterations = CONFIG_MAX_ITERATIONS.get(configs);
        if (maxIterations > SYSTEM_MAX_ITERATIONS) {
            throw new FrontendException(
                    Code.LOGICAL_PLAN_BUILD_FAILED,
                    "max iterations "
                            + maxIterations
                            + " exceeds the system limit "
                            + SYSTEM_MAX_ITERATIONS);
        }
        this.maxIterations = maxIterations;
    }

    public boolean validate(LogicalPlan plan, boolean throwsOnFail) {
        if (plan.getRegularQuery() == null || plan.isReturnEmpty()) return true;
        int hops = 0;
        List<RelNode> queue = Lists.newArrayList(plan.getRegularQuery());
        while (!queue.isEmpty()) {
            RelNode top = queue.remove(0);
            if (top instanceof GraphLogicalExpand) {
                ++hops;
            } else if (top instanceof GraphPhysicalExpand) {
                ++hops;
            } else if (top instanceof GraphLogicalPathExpand) {
                GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) top;
                // null means no limit
                if (pxd.getFetch() == null) {
                    if (throwsOnFail) {
                        throw new FrontendException(
                                Code.LOGICAL_PLAN_BUILD_FAILED,
                                "path expand with no upper bound exceeds the maximum allowed"
                                        + " iterations "
                                        + maxIterations);
                    }
                    return false;
                }
                int lower =
                        (pxd.getOffset() == null)
                                ? 0
                                : ((Number) ((RexLiteral) pxd.getOffset()).getValue()).intValue();
                hops +=
                        (lower
                                + ((Number) ((RexLiteral) pxd.getFetch()).getValue()).intValue()
                                - 1);
            } else if (top instanceof GraphLogicalSingleMatch) {
                validate(
                        new LogicalPlan(((GraphLogicalSingleMatch) top).getSentence()),
                        throwsOnFail);
            } else if (top instanceof GraphLogicalMultiMatch) {
                for (RelNode sentence : ((GraphLogicalMultiMatch) top).getSentences()) {
                    validate(new LogicalPlan(sentence), throwsOnFail);
                }
            } else if (top instanceof CommonTableScan) {
                CommonOptTable optTable = (CommonOptTable) top.getTable();
                validate(new LogicalPlan(optTable.getCommon()), throwsOnFail);
            }
            queue.addAll(top.getInputs());
        }
        if (hops <= maxIterations) return true;
        if (throwsOnFail) {
            throw new FrontendException(
                    Code.LOGICAL_PLAN_BUILD_FAILED,
                    "query hops "
                            + hops
                            + " exceeds the maximum allowed iterations "
                            + maxIterations);
        }
        return false;
    }
}
