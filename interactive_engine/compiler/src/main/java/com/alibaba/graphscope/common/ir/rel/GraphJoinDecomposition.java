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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.type.JoinVertexEntry;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GraphJoinDecomposition extends BiRel {
    private final List<JoinVertexPair> joinVertexPairs;
    private final OrderMappings orderMappings;
    private final Pattern parentPatten;
    private final Pattern probePattern;
    private final Pattern buildPattern;
    private final JoinRelType joinType;

    public GraphJoinDecomposition(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            Pattern parentPattern,
            Pattern probePattern,
            Pattern buildPattern,
            List<JoinVertexPair> joinVertexPairs,
            OrderMappings orderMappings) {
        this(
                cluster,
                traitSet,
                parentPattern,
                probePattern,
                buildPattern,
                joinVertexPairs,
                orderMappings,
                JoinRelType.INNER);
    }

    public GraphJoinDecomposition(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            Pattern parentPattern,
            Pattern probePattern,
            Pattern buildPattern,
            List<JoinVertexPair> joinVertexPairs,
            OrderMappings orderMappings,
            JoinRelType joinType) {
        this(
                cluster,
                traitSet,
                parentPattern,
                new GraphPattern(cluster, traitSet, probePattern),
                probePattern,
                new GraphPattern(cluster, traitSet, buildPattern),
                buildPattern,
                joinVertexPairs,
                orderMappings,
                joinType);
    }

    public GraphJoinDecomposition(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            Pattern parentPattern,
            RelNode left,
            Pattern leftPattern,
            RelNode right,
            Pattern rightPattern,
            List<JoinVertexPair> joinVertexPairs,
            OrderMappings orderMappings,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right);
        this.parentPatten = parentPattern;
        this.joinVertexPairs = joinVertexPairs;
        this.orderMappings = orderMappings;
        this.probePattern = leftPattern;
        this.buildPattern = rightPattern;
        this.joinType = joinType;
    }

    public List<JoinVertexPair> getJoinVertexPairs() {
        return Collections.unmodifiableList(this.joinVertexPairs);
    }

    public OrderMappings getOrderMappings() {
        return orderMappings;
    }

    public Pattern getParentPatten() {
        return parentPatten;
    }

    public Pattern getProbePattern() {
        return probePattern;
    }

    public Pattern getBuildPattern() {
        return buildPattern;
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    public static class JoinVertexPair
            extends Pair<JoinVertexEntry<Integer>, JoinVertexEntry<Integer>> {
        private final boolean isForeignKey;

        public JoinVertexPair(
                JoinVertexEntry<Integer> left,
                JoinVertexEntry<Integer> right,
                boolean isForeignKey) {
            super(left, right);
            this.isForeignKey = isForeignKey;
        }

        public int getLeftOrderId() {
            return left.getVertex();
        }

        public int getRightOrderId() {
            return right.getVertex();
        }

        public boolean isForeignKey() {
            return isForeignKey;
        }

        @Override
        public String toString() {
            return "JoinVertexPair{"
                    + "leftOrderId="
                    + getLeftOrderId()
                    + ", rightOrderId="
                    + getRightOrderId()
                    + '}';
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

        @Override
        public String toString() {
            return "OrderMappings{"
                    + "leftToTargetOrderMap="
                    + leftToTargetOrderMap
                    + ", rightToTargetOrderMap="
                    + rightToTargetOrderMap
                    + '}';
        }
    }

    @Override
    public RelDataType deriveRowType() {
        return getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY);
    }

    @Override
    public GraphJoinDecomposition copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GraphJoinDecomposition(
                getCluster(),
                traitSet,
                parentPatten,
                inputs.get(0),
                this.probePattern,
                inputs.get(1),
                this.buildPattern,
                this.joinVertexPairs,
                this.orderMappings,
                this.joinType);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return super.accept(shuttle);
    }

    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("jointVertices", joinVertexPairs)
                .item("orderMappings", orderMappings);
    }
}
