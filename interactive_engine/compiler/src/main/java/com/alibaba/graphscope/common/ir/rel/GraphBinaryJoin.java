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

package com.alibaba.graphscope.common.ir.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GraphBinaryJoin extends BiRel {
    private final List<JoinVertexPair> joinVertexPairs;
    private final OrderMappings orderMappings;

    protected GraphBinaryJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            List<JoinVertexPair> joinVertexPairs,
            OrderMappings orderMappings) {
        super(cluster, traitSet, left, right);
        this.joinVertexPairs = joinVertexPairs;
        this.orderMappings = orderMappings;
    }

    public static class JoinVertexPair {
        private final int leftOrderId;
        private final int rightOrderId;

        public JoinVertexPair(int leftOrderId, int rightOrderId) {
            this.leftOrderId = leftOrderId;
            this.rightOrderId = rightOrderId;
        }

        public int getLeftOrderId() {
            return leftOrderId;
        }

        public int getRightOrderId() {
            return rightOrderId;
        }
    }

    public static class OrderMappings {
        private final Map<Integer, Integer> leftToTargetOrderMap;
        private final Map<Integer, Integer> rightToTargetOrderMap;

        public OrderMappings(
                Map<Integer, Integer> leftToTargetOrderMap,
                Map<Integer, Integer> rightToTargetOrderMap) {
            this.leftToTargetOrderMap = leftToTargetOrderMap;
            this.rightToTargetOrderMap = rightToTargetOrderMap;
        }

        public Map<Integer, Integer> getLeftToTargetOrderMap() {
            return Collections.unmodifiableMap(this.leftToTargetOrderMap);
        }

        public Map<Integer, Integer> getRightToTargetOrderMap() {
            return Collections.unmodifiableMap(this.rightToTargetOrderMap);
        }
    }
}
