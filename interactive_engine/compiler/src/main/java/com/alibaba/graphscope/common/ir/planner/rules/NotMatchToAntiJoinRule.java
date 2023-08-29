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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class NotMatchToAntiJoinRule<C extends NotMatchToAntiJoinRule.Config> extends RelRule<C>
        implements TransformationRule {

    protected NotMatchToAntiJoinRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
        List<RexNode> conditionsToRemove = Lists.newArrayList();
        RelNode input = filter.getInput();
        GraphBuilder graphBuilder = (GraphBuilder) call.builder();
        for (RexNode condition : conditions) {
            if (foundNotExistSubQuery(condition)) {
                input = convertToAntiJoin(input, condition, graphBuilder);
                conditionsToRemove.add(condition);
            }
        }
        conditions.removeAll(conditionsToRemove);
        if (!conditionsToRemove.isEmpty() && !conditions.isEmpty()) {
            filter = (Filter) graphBuilder.push(input).filter(conditions).build();
        }
        RelNode newNode = (conditions.isEmpty()) ? input : filter;
        call.transformTo(newNode);
    }

    private RelNode convertToAntiJoin(
            RelNode input, RexNode notExistCondition, GraphBuilder builder) {
        RelNode leftChild = input;
        RexSubQuery existSubQuery = (RexSubQuery) ((RexCall) notExistCondition).operands.get(0);
        RelNode rightChild = builder.match(existSubQuery.rel, GraphOpt.Match.INNER).build();
        return builder.push(leftChild)
                .push(rightChild)
                .antiJoin(builder.getJoinCondition(leftChild, rightChild))
                .build();
    }

    public static class Config implements RelRule.Config {
        public static NotMatchToAntiJoinRule.Config DEFAULT =
                new NotMatchToAntiJoinRule.Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Filter.class)
                                                .predicate(
                                                        k -> {
                                                            List<RexNode> conditions =
                                                                    RelOptUtil.conjunctions(
                                                                            k.getCondition());
                                                            for (RexNode condition : conditions) {
                                                                if (foundNotExistSubQuery(
                                                                        condition)) {
                                                                    return true;
                                                                }
                                                            }
                                                            return false;
                                                        })
                                                .anyInputs())
                        .withDescription("NotExistToAntiJoinRule");

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new NotMatchToAntiJoinRule(this);
        }

        @Override
        public NotMatchToAntiJoinRule.Config withRelBuilderFactory(
                RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public NotMatchToAntiJoinRule.Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public NotMatchToAntiJoinRule.Config withOperandSupplier(
                RelRule.OperandTransform operandTransform) {
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

    private static boolean foundNotExistSubQuery(RexNode condition) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.NOT) {
                RexNode operand = ((RexCall) condition).operands.get(0);
                return operand instanceof RexSubQuery
                        && ((RexSubQuery) operand).getOperator().getKind() == SqlKind.EXISTS;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
