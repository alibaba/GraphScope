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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TopKPushDownRule extends SortProjectTransposeRule {
    protected TopKPushDownRule(Config config) {
        super(config);
    }

    public static class Config implements SortProjectTransposeRule.Config {
        public static RelRule.Config DEFAULT =
                // the sort is the limit operator
                new TopKPushDownRule.Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(GraphLogicalSort.class)
                                                .predicate(
                                                        (GraphLogicalSort sort) ->
                                                                sort.getCollation()
                                                                        .getFieldCollations()
                                                                        .isEmpty())
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                GraphLogicalProject
                                                                                        .class)
                                                                        .anyInputs()))
                        .as(Config.class);

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public TopKPushDownRule toRule() {
            return new TopKPushDownRule(this);
        }

        @Override
        public Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public Config withOperandSupplier(OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        @Override
        public OperandTransform operandSupplier() {
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
