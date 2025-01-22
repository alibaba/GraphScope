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
    public static int MAX_ITERATIONS = 15;

    boolean validate(LogicalPlan plan, boolean throwsOnFail) {
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
                                        + MAX_ITERATIONS);
                    }
                    return false;
                }
                hops += ((Number) ((RexLiteral) pxd.getFetch()).getValue()).intValue();
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
        if (hops <= MAX_ITERATIONS) return true;
        if (throwsOnFail) {
            throw new FrontendException(
                    Code.LOGICAL_PLAN_BUILD_FAILED,
                    "query hops "
                            + hops
                            + " exceeds the maximum allowed iterations "
                            + MAX_ITERATIONS);
        }
        return false;
    }
}
