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
 * the detailed estimated cost of {@code Expand} operator
 */
public class DetailedExpandCost extends RelOptCostImpl {
    // the total count of all edges with type <src, edge>
    private final double expandRows;
    // expandRows * the selectivity of the edge
    private final double expandFilteringRows;
    // the total count of all edges with type <src, edge, dst> * the selectivity of the edge
    private final double getVRows;
    // getVCost * the selectivity of the dst vertex
    private final double getVFilteringRows;

    public DetailedExpandCost(
            double expandRows,
            double expandFilteringRows,
            double getVRows,
            double getVFilteringRows) {
        super(expandRows);
        this.expandRows = expandRows;
        this.expandFilteringRows = expandFilteringRows;
        this.getVRows = getVRows;
        this.getVFilteringRows = getVFilteringRows;
    }

    public double getExpandRows() {
        return expandRows;
    }

    public double getExpandFilteringRows() {
        return expandFilteringRows;
    }

    public double getGetVRows() {
        return getVRows;
    }

    public double getGetVFilteringRows() {
        return getVFilteringRows;
    }

    @Override
    public String toString() {
        return "DetailedExpandCost{"
                + "expandRows="
                + expandRows
                + ", expandFilteringRows="
                + expandFilteringRows
                + ", getVRows="
                + getVRows
                + ", getVFilteringRows="
                + getVFilteringRows
                + '}';
    }
}
