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

package com.alibaba.graphscope.common.ir.planner.volcano;

import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class VolcanoPlannerX extends VolcanoPlanner {
    @Override
    protected RelOptCost upperBoundForInputs(RelNode mExpr, RelOptCost upperBound) {
        // todo: support pruning optimizations
        return upperBound;
    }

    @Override
    public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        Preconditions.checkArgument(rel != null, "rel is null");
        if (rel instanceof RelSubset) {
            return Utils.getFieldValue(RelSubset.class, rel, "bestCost");
        }
        boolean noneConventionHasInfiniteCost =
                Utils.getFieldValue(VolcanoPlanner.class, this, "noneConventionHasInfiniteCost");
        if (noneConventionHasInfiniteCost
                && rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE) {
            return costFactory.makeInfiniteCost();
        }
        RelOptCost cost = this.costFactory.makeZeroCost();
        for (RelNode input : rel.getInputs()) {
            RelOptCost inputCost = getCost(input, mq);
            if (inputCost == null || inputCost.isInfinite()) {
                return inputCost;
            }
            cost = cost.plus(inputCost);
        }
        RelOptCost relCost = mq.getNonCumulativeCost(rel);
        if (relCost == null) return null;
        return cost.plus(relCost);
    }

    @Override
    public void registerSchema(RelOptSchema schema) {
        // do nothing
    }
}
