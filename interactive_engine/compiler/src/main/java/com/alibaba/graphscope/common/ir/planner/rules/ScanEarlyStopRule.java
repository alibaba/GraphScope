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

import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.QueryParams;
import com.alibaba.graphscope.common.ir.rel.RangeParam;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pushes the limit operation down to the scan node.
 * During the scan process, the scan stops as soon as the specified limit count is reached.
 */
public class ScanEarlyStopRule extends RelRule {
    protected ScanEarlyStopRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        // fuse limit to source
        GraphLogicalSort sort = relOptRuleCall.rel(0);
        GraphLogicalSource source = relOptRuleCall.rel(1);
        RangeParam range = new RangeParam(sort.offset, sort.fetch);
        GraphLogicalSource fused =
                GraphLogicalSource.create(
                        (GraphOptCluster) source.getCluster(),
                        source.getHints(),
                        source.getOpt(),
                        source.getTableConfig(),
                        source.getAliasName(),
                        new QueryParams().addParam("range", range),
                        source.getUniqueKeyFilters(),
                        source.getFilters());
        relOptRuleCall.transformTo(fused);
    }

    public static class Config implements RelRule.Config {
        public static RelRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(GraphLogicalSort.class)
                                                .predicate(
                                                        (GraphLogicalSort sort) -> {
                                                            // check sort is the limit
                                                            return sort.getCollation()
                                                                    .getFieldCollations()
                                                                    .isEmpty();
                                                        })
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(GraphLogicalSource.class)
                                                                        .anyInputs()));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new ScanEarlyStopRule(this);
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
