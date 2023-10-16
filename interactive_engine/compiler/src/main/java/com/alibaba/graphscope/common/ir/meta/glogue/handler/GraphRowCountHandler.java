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

package com.alibaba.graphscope.common.ir.meta.glogue.handler;

import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

class GraphRowCountHandler implements BuiltInMetadata.RowCount.Handler {
    private final RelMdRowCount mdRowCount;
    private final GlogueQuery glogueQuery;
    private final RelOptPlanner optPlanner;

    public GraphRowCountHandler(
            RelOptPlanner optPlanner, RelMdRowCount mdRowCount, GlogueQuery glogueQuery) {
        this.optPlanner = optPlanner;
        this.mdRowCount = mdRowCount;
        this.glogueQuery = glogueQuery;
    }

    @Override
    public Double getRowCount(RelNode node, RelMetadataQuery mq) {
        if (node instanceof GraphPattern) {
            // todo: 1. estimate the pattern graph which size is greater than the glogue's upper
            // bound
            // todo: 2. estimate the pattern graph with filter conditions
            return glogueQuery.getRowCount(((GraphPattern) node).getPattern());
        } else if (node instanceof Filter) {
            return mdRowCount.getRowCount((Filter) node, mq);
        } else if (node instanceof Aggregate) {
            return mdRowCount.getRowCount((Aggregate) node, mq);
        } else if (node instanceof Sort) {
            return mdRowCount.getRowCount((Sort) node, mq);
        } else if (node instanceof Project) {
            return mdRowCount.getRowCount((Project) node, mq);
        } else if (node instanceof RelSubset) {
            return mdRowCount.getRowCount((RelSubset) node, mq);
        } else if (node instanceof Join) {
            return mdRowCount.getRowCount((Join) node, mq);
        } else if (node instanceof Union) {
            return mdRowCount.getRowCount((Union) node, mq);
        } else if (node instanceof GraphExtendIntersect) {
            if (optPlanner instanceof VolcanoPlanner) {
                RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                if (subset != null) {
                    RelNode currentPattern = subset.getOriginal();
                    // use the row count of the current pattern to estimate the communication cost
                    return mq.getRowCount(currentPattern);
                }
            }
        }
        throw new IllegalArgumentException("can not estimate row count for the node=" + node);
    }
}
