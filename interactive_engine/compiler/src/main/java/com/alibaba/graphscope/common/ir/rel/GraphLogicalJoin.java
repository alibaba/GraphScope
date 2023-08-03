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

import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * extend {@link Join} in calcite to re-implement {@link #deriveRowType()}
 */
public class GraphLogicalJoin extends Join {
    protected GraphLogicalJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    public static GraphLogicalJoin create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinRelType) {
        return new GraphLogicalJoin(
                cluster,
                RelTraitSet.createEmpty(),
                hints,
                left,
                right,
                condition,
                variablesSet,
                joinRelType);
    }

    // for a join b, calcite will keep duplicated fields in both a and b while we need to dedup them
    @Override
    protected RelDataType deriveRowType() {
        RelDataType leftType = this.getLeft().getRowType();
        RelDataType rightType = this.getRight().getRowType();
        List<RelDataTypeField> joinFields = Lists.newArrayList();
        leftType.getFieldList().forEach(k -> {
            if (!k.getName().equals(AliasInference.DEFAULT_NAME)) {
                joinFields.add(k);
            }
        });
        rightType.getFieldList().forEach(k -> {
            if (!k.getName().equals(AliasInference.DEFAULT_NAME)) {
                joinFields.add(k);
            }
        });
        return new RelRecordType(StructKind.FULLY_QUALIFIED, joinFields.stream().distinct().collect(Collectors.toList()));
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinRelType, boolean isSemiJoinDone) {
        return new GraphLogicalJoin(
                getCluster(),
                traitSet,
                this.hints,
                left,
                right,
                condition,
                getVariablesSet(),
                joinRelType);
    }
}
