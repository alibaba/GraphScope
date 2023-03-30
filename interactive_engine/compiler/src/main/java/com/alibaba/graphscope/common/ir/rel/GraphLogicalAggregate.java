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

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class GraphLogicalAggregate extends Aggregate {
    private GraphGroupKeys groupKey;
    private List<GraphAggCall> aggCalls;

    protected GraphLogicalAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            GraphGroupKeys groupKey,
            List<GraphAggCall> aggCalls) {
        super(cluster, traitSet, hints, input, ImmutableBitSet.of(), null, ImmutableList.of());
        Objects.requireNonNull(input);
        this.groupKey = Objects.requireNonNull(groupKey);
        // if empty -> the aggregate operator is a dedup
        this.aggCalls = Objects.requireNonNull(aggCalls);
    }

    public static GraphLogicalAggregate create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphGroupKeys groupKey,
            List<GraphAggCall> aggCalls) {
        return new GraphLogicalAggregate(
                cluster, RelTraitSet.createEmpty(), hints, input, groupKey, aggCalls);
    }

    @Override
    protected RelDataType deriveRowType() {
        List<RexNode> rexNodes = new ArrayList<>();
        List<@Nullable String> aliases = new ArrayList<>();
        if (ObjectUtils.isNotEmpty(groupKey.getVariables())) {
            rexNodes.addAll(groupKey.getVariables());
            aliases.addAll(groupKey.getAliases());
            while (aliases.size() < rexNodes.size()) {
                aliases.add(null);
            }
        }
        for (GraphAggCall aggCall : aggCalls) {
            rexNodes.add(aggCall.rexCall());
            aliases.add(aggCall.getAlias());
        }
        List<RelDataTypeField> fields = new ArrayList<>();
        if (ObjectUtils.isNotEmpty(rexNodes)) {
            List<String> aliasList =
                    AliasInference.inferProject(rexNodes, aliases, new HashSet<>());
            assert aliasList.size() == rexNodes.size();
            for (int i = 0; i < rexNodes.size(); ++i) {
                String aliasName = aliasList.get(i);
                RelOptCluster cluster = getCluster();
                int aliasId = ((GraphOptCluster) cluster).getIdGenerator().generate(aliasName);
                fields.add(new RelDataTypeFieldImpl(aliasName, aliasId, rexNodes.get(i).getType()));
            }
            // update aliases in groupKey
            this.groupKey =
                    new GraphGroupKeys(
                            this.groupKey.getVariables(),
                            aliasList.subList(0, this.groupKey.groupKeyCount()));
            // update alias in each groupValue
            List<GraphAggCall> copyCalls = new ArrayList<>();
            int offset = this.groupKey.groupKeyCount();
            for (int i = 0; i < aggCalls.size(); ++i) {
                copyCalls.add(aggCalls.get(i).copy(aliasList.get(offset + i)));
            }
            this.aggCalls = copyCalls;
        }
        return new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
    }

    @Override
    public Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            @Nullable List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new GraphLogicalAggregate(
                getCluster(), traitSet, getHints(), input, this.groupKey, this.aggCalls);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("input", input).item("keys", groupKey).item("values", aggCalls);
    }

    public GraphGroupKeys getGroupKey() {
        return groupKey;
    }

    public List<GraphAggCall> getAggCalls() {
        return Collections.unmodifiableList(aggCalls);
    }
}
