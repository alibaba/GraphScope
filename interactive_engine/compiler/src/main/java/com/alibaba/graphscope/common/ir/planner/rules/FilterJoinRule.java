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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterJoinRule<C extends FilterJoinRule.Config> extends RelRule<C>
        implements TransformationRule {

    protected FilterJoinRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        Filter filter = relOptRuleCall.rel(0);
        Join join = relOptRuleCall.rel(1);
        perform(relOptRuleCall, filter, join);
    }

    private void perform(RelOptRuleCall call, Filter filter, Join join) {
        List<RexNode> originalFilters = RelOptUtil.conjunctions(filter.getCondition());
        List<RexNode> aboveFilters = Lists.newArrayList(filter.getCondition());
        List<RexNode> leftFilters = Lists.newArrayList();
        List<RexNode> rightFilters = Lists.newArrayList();
        JoinRelType joinRelType = join.getJoinType();
        if (!classifyFilter(join, aboveFilters, joinRelType.canPushLeftFromAbove(), joinRelType.canPushRightFromAbove(), leftFilters, rightFilters)
            && leftFilters.isEmpty()
                && rightFilters.isEmpty()) {
            return;
        }
        GraphBuilder graphBuilder = (GraphBuilder) call.builder();
        RelNode newLeft = leftFilters.isEmpty() ? join.getLeft() : graphBuilder.push(join.getLeft()).filter(leftFilters).build();
        RelNode newRight = rightFilters.isEmpty() ? join.getRight() : graphBuilder.push(join.getRight()).filter(rightFilters).build();
        RelNode newJoin = join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(), join.isSemiJoinDone());
        if (!aboveFilters.isEmpty() && !Sets.newHashSet(originalFilters).equals(Sets.newHashSet(aboveFilters))) {
            filter = (Filter) graphBuilder.push(newJoin).filter(aboveFilters).build();
        }
        RelNode newNode = (aboveFilters.isEmpty()) ? newJoin : filter;
        call.transformTo(newNode);
    }

    private boolean classifyFilter(RelNode joinRel,
                                   List<RexNode> filters,
                                   boolean pushLeft,
                                   boolean pushRight,
                                   List<RexNode> leftFilters,
                                   List<RexNode> rightFilters) {
        List<RexNode> filtersToRemove = Lists.newArrayList();
        for (RexNode filter : filters) {
            Set<Integer> distinctAliasIds =
                    filter
                            .accept(new RexVariableAliasCollector<>(true, RexGraphVariable::getAliasId))
                            .stream()
                            .collect(Collectors.toSet());
            if (pushLeft && containsAliasIds(joinRel.getInput(0).getRowType(), distinctAliasIds)) {
                leftFilters.add(filter);
                filtersToRemove.add(filter);
            } else if (pushRight && containsAliasIds(joinRel.getInput(1).getRowType(), distinctAliasIds)) {
                rightFilters.add(filter);
                filtersToRemove.add(filter);
            }
        }
        if (!filtersToRemove.isEmpty()) {
            filters.removeAll(filtersToRemove);
        }
        return !filtersToRemove.isEmpty();
    }

    private boolean containsAliasIds(RelDataType rowDataType, Set<Integer> aliasIds) {
        return rowDataType.getFieldList().stream().map(k -> k.getIndex()).collect(Collectors.toSet()).containsAll(aliasIds);
    }

    public static class Config implements RelRule.Config {
        public static FilterJoinRule.Config DEFAULT =
                new FilterJoinRule.Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Filter.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(Join.class)
                                                                        .anyInputs()))
                        .withRelBuilderFactory(
                                (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                                        GraphBuilder.create(
                                                null, (GraphOptCluster) cluster, schema));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new FilterJoinRule(this);
        }

        @Override
        public FilterJoinRule.Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public FilterJoinRule.Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public FilterJoinRule.Config withOperandSupplier(RelRule.OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        @Override
        public RelRule.OperandTransform operandSupplier() {
            return this.operandSupplier;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
            return this.description;
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            return this.builderFactory;
        }
    }
}
