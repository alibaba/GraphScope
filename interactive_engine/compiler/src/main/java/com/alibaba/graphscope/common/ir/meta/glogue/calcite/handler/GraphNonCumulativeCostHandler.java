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

package com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler;

import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class GraphNonCumulativeCostHandler implements BuiltInMetadata.NonCumulativeCost.Handler {
    private final RelOptPlanner optPlanner;
    private final RelOptCostFactory costFactory;

    public GraphNonCumulativeCostHandler(RelOptPlanner optPlanner) {
        this.optPlanner = optPlanner;
        this.costFactory = optPlanner.getCostFactory();
    }
    /**
     * estimate the non-cumulative cost of {@code GraphExtendIntersect} or {@code GraphBinaryJoin} operator
     * @param node
     * @param mq
     * @return
     */
    @Override
    public RelOptCost getNonCumulativeCost(RelNode node, RelMetadataQuery mq) {
        if (node instanceof GraphExtendIntersect) {
            GlogueExtendIntersectEdge glogueEdge = ((GraphExtendIntersect) node).getGlogueEdge();
            double weight = glogueEdge.getExtendStep().getWeight();
            RelNode input = node.getInput(0);
            double srcPatternCount =
                    mq.getRowCount(
                            input instanceof RelSubset ? ((RelSubset) input).getOriginal() : input);
            double dRows = weight * srcPatternCount;
            double dCpu = dRows + 1;
            double dIo = 0;
            if (optPlanner instanceof VolcanoPlanner) {
                RelSubset subset = ((VolcanoPlanner) optPlanner).getSubset(node);
                if (subset != null) {
                    RelNode currentPattern = subset.getOriginal();
                    // use the row count of the current pattern to estimate the communication cost
                    dIo = mq.getRowCount(currentPattern);
                }
            }
            return costFactory.makeCost(dRows, dCpu, dIo);
        } else if (node instanceof GraphPattern) {
            int patternSize = ((GraphPattern) node).getPattern().getVertexNumber();
            if (patternSize <= 1) {
                double dRows = mq.getRowCount(node);
                return costFactory.makeCost(dRows, dRows + 1, dRows);
            }
            return costFactory.makeInfiniteCost();
        } else {
            // todo: estimate the row count of GraphBinaryJoin
            return node.computeSelfCost(node.getCluster().getPlanner(), mq);
        }
    }
}
