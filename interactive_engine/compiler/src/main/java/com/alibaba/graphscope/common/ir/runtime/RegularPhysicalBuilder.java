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

import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * build physical plan from logical plan of a regular query
 * @param <T>
 * @param <R>
 */
public abstract class RegularPhysicalBuilder<T, R> extends PhysicalBuilder<R> {
    protected RelShuttle relShuttle;

    protected RegularPhysicalBuilder(LogicalPlan logicalPlan, RelShuttle relShuttle) {
        super(logicalPlan);
        this.relShuttle = relShuttle;
    }

    protected void initialize() {
        if (this.logicalPlan.getRegularQuery() != null && !this.logicalPlan.isReturnEmpty()) {
            RelNode regularQuery = this.logicalPlan.getRegularQuery();
            RelVisitor relVisitor =
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            super.visit(node, ordinal, parent);
                            appendNode((PhysicalNode) node.accept(relShuttle));
                        }
                    };
            relVisitor.go(regularQuery);
        }
    }

    /**
     * append {@code PhysicalNode} to the physical plan
     * @param node
     */
    protected abstract void appendNode(PhysicalNode<T> node);
}
