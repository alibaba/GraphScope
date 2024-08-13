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

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

public class GraphNonCumulativeCostHandler implements BuiltInMetadata.NonCumulativeCost.Handler {
    private final RelOptPlanner optPlanner;
    private final RelOptCostFactory costFactory;
    private final PlannerConfig plannerConfig;

    public GraphNonCumulativeCostHandler(RelOptPlanner optPlanner, PlannerConfig plannerConfig) {
        this.optPlanner = optPlanner;
        this.costFactory = optPlanner.getCostFactory();
        this.plannerConfig = plannerConfig;
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
            double srcPatternCount = mq.getRowCount(node.getInput(0));
            double dRows = weight * srcPatternCount;
            if (glogueEdge.getExtendStep().getExtendEdges().size() > 1) {
                dRows *= plannerConfig.getIntersectCostFactor();
            }
            double dCpu = dRows + 1;
            double dIo = mq.getRowCount(node);
            return costFactory.makeCost(dRows, dCpu, dIo);
        } else if (node instanceof GraphPattern) {
            int patternSize = ((GraphPattern) node).getPattern().getVertexNumber();
            if (patternSize <= 1) {
                double dRows = mq.getRowCount(node);
                return costFactory.makeCost(dRows, dRows + 1, dRows);
            }
            return costFactory.makeInfiniteCost();
        } else if (node instanceof GraphJoinDecomposition) {
            // estimate the cost of join operators,
            // here we assume the underline join operator is hash join, thus the cost is
            // w1*count(left)+w2*count(right), where w1 and w2 are constant
            GraphJoinDecomposition decomposition = (GraphJoinDecomposition) node;
            double probeCount = mq.getRowCount(decomposition.getLeft());
            double buildCount = mq.getRowCount(decomposition.getRight());
            double dRows;
            List<GraphJoinDecomposition.JoinVertexPair> joinVertexPairs =
                    decomposition.getJoinVertexPairs();
            // foreign key join
            if (joinVertexPairs.stream().allMatch(k -> k.isForeignKey())) {
                dRows = Math.min(probeCount, buildCount) * 2;
            } else {
                dRows =
                        plannerConfig.getJoinCostFactor1() * probeCount
                                + plannerConfig.getJoinCostFactor2() * buildCount;
            }
            double dCpu = dRows + 1;
            double dIo = mq.getRowCount(node);
            return costFactory.makeCost(dRows, dCpu, dIo);
        } else {
            return node.computeSelfCost(optPlanner, mq);
        }
    }
}
