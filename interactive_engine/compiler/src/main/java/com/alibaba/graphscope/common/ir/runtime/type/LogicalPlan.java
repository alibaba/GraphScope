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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public abstract class LogicalPlan<T, R> extends AbstractRelNode implements AutoCloseable {
    protected final List<RelHint> hints;
    protected boolean returnEmpty;

    protected LogicalPlan(RelOptCluster cluster, List<RelHint> hints) {
        super(cluster, RelTraitSet.createEmpty());
        this.hints = hints;
    }

    /**
     * append {@code LogicalNode} to the plan
     * @param node
     */
    public abstract void appendNode(LogicalNode<T> node);

    /**
     * output logical plan
     */
    public abstract String explain();

    /**
     * convert logical plan to physical plan
     * @return
     */
    public abstract R toPhysical();

    public void setReturnEmpty(boolean returnEmpty) {
        this.returnEmpty = returnEmpty;
    }

    public boolean isReturnEmpty() {
        return returnEmpty;
    }
}
