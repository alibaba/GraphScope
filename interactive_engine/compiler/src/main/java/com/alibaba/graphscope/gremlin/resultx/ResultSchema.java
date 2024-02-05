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

package com.alibaba.graphscope.gremlin.resultx;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class ResultSchema {
    public final RelDataType outputType;
    public final boolean isGroupBy;
    public final int groupKeyCount;

    public ResultSchema(LogicalPlan plan) {
        this.outputType = plan.getOutputType();
        GraphLogicalAggregate aggregate = tailAggregate(plan);
        this.isGroupBy = aggregate != null;
        this.groupKeyCount = getGroupKeyCount(aggregate);
    }

    private @Nullable GraphLogicalAggregate tailAggregate(LogicalPlan logicalPlan) {
        RelNode top = logicalPlan.getRegularQuery();
        List<RelNode> queue = Lists.newArrayList(top);
        while (!queue.isEmpty()) {
            RelNode node = queue.remove(0);
            if (!node.getInputs().isEmpty()
                    && node.getInput(0).getRowType().equals(node.getRowType())) {
                queue.addAll(node.getInputs());
            } else if (node instanceof GraphLogicalAggregate
                    && !((GraphLogicalAggregate) node).getAggCalls().isEmpty()
                    && ((GraphLogicalAggregate) node).getGroupKey().groupKeyCount() != 0) {
                return (GraphLogicalAggregate) node;
            }
        }
        return null;
    }

    private int getGroupKeyCount(@Nullable GraphLogicalAggregate aggregate) {
        if (aggregate == null) return 0;
        return aggregate.getGroupKey().groupKeyCount();
    }
}
