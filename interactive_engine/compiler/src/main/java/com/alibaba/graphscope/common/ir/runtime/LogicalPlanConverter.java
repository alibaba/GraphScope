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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.ir.runtime.type.LogicalNode;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalPlan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalValues;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * convert logical plan in calcite to the structures in ir_core
 * @param <T> type of each node (i.e. {@link com.sun.jna.Pointer} for FFI interfaces)
 * @param <R> type of physical plan (i.e. {@link com.alibaba.graphscope.common.jna.type.FfiData for FFI interfaces})
 */
public class LogicalPlanConverter<T, R> extends RelVisitor {
    private final LogicalPlan<T, R> logicalPlan;
    private final RelShuttle rexShuttle;

    public LogicalPlanConverter(RelShuttle relShuttle, LogicalPlan<T, R> logicalPlan) {
        this.rexShuttle = Objects.requireNonNull(relShuttle);
        this.logicalPlan = Objects.requireNonNull(logicalPlan);
    }

    @Override
    public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        super.visit(node, ordinal, parent);
        if (node instanceof LogicalValues) {
            this.logicalPlan.setReturnEmpty(true);
        }
        if (!this.logicalPlan.isReturnEmpty()) {
            this.logicalPlan.appendNode((LogicalNode) node.accept(rexShuttle));
        }
    }

    @Override
    public LogicalPlan<T, R> go(RelNode p) {
        replaceRoot(p);
        visit(p, 0, null);
        return this.logicalPlan;
    }
}
