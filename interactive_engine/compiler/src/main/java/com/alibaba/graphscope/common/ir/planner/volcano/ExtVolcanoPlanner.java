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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;

public class ExtVolcanoPlanner extends VolcanoPlanner {
    public ExtVolcanoPlanner() {
        super();
    }

    @Override
    protected RelOptCost upperBoundForInputs(RelNode mExpr, RelOptCost upperBound) {
        RelSubset group = getSubset(mExpr);
        RelOptCost bestCost =
                (group != null) ? Utils.getFieldValue(RelSubset.class, group, "bestCost") : null;
        RelOptCost currentUpperBound =
                (bestCost == null || upperBound != null && upperBound.isLt(bestCost))
                        ? upperBound
                        : bestCost;
        if (currentUpperBound != null && !currentUpperBound.isInfinite()) {
            RelOptCost rootCost = mExpr.getCluster().getMetadataQuery().getNonCumulativeCost(mExpr);
            if (rootCost != null && !rootCost.isInfinite()) {
                return currentUpperBound.minus(rootCost);
            }
        }
        return upperBound;
    }

    @Override
    public void setRoot(RelNode rel) {
        super.setRoot(rel);
    }

    @Override
    public RelNode findBestExp() {
        return super.findBestExp();
    }
}
