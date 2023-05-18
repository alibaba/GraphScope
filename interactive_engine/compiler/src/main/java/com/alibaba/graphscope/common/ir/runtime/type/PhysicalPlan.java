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

package com.alibaba.graphscope.common.ir.runtime.type;

import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalPlan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

/**
 * define interfaces to build a physical plan, {@link FfiPhysicalPlan} is one of implementations
 * @param <T>
 * @param <R>
 */
public abstract class PhysicalPlan<T, R> extends AbstractRelNode implements AutoCloseable {
    protected PhysicalPlan(RelOptCluster cluster) {
        super(cluster, RelTraitSet.createEmpty());
    }

    /**
     * append {@code PhysicalNode} to the plan
     * @param node
     */
    public abstract void appendNode(PhysicalNode<T> node);

    /**
     * print physical plan
     */
    public abstract String explain();

    /**
     * build physical plan
     * @return
     */
    public abstract R build();
}
