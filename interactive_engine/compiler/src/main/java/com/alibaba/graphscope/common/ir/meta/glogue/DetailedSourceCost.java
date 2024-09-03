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

package com.alibaba.graphscope.common.ir.meta.glogue;

import org.apache.calcite.plan.RelOptCostImpl;

/**
 * the detailed estimated cost of {@code Source}
 */
public class DetailedSourceCost extends RelOptCostImpl {
    // the total count of all nodes within the type constraints
    private final double labelFilteringRows;
    // labelFilteringRows * the selectivity of the filtering
    private final double predicateFilteringRows;

    public DetailedSourceCost(double labelFilteringRows, double predicateFilteringRows) {
        super(predicateFilteringRows);
        this.labelFilteringRows = labelFilteringRows;
        this.predicateFilteringRows = predicateFilteringRows;
    }

    public double getLabelFilteringRows() {
        return labelFilteringRows;
    }

    public double getPredicateFilteringRows() {
        return predicateFilteringRows;
    }

    @Override
    public String toString() {
        return "DetailedSourceCost{"
                + "labelFilteringRows="
                + labelFilteringRows
                + ", predicateFilteringRows="
                + predicateFilteringRows
                + '}';
    }
}
