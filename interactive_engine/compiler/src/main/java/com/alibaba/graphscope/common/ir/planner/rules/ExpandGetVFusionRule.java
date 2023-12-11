/*
 * Copyright 2023 Alibaba Group Holding Limited.
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

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalExpandGetV;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExpandGetVFusionRule<C extends ExpandGetVFusionRule.Config> extends RelRule<C>
        implements TransformationRule {

    protected ExpandGetVFusionRule(C config) {
        super(config);
    }

    protected RelNode transform(
            GraphLogicalGetV getV, GraphLogicalExpand expand, GraphBuilder builder) {
        RelNode expandGetV =
                GraphPhysicalExpandGetV.create(
                        (GraphOptCluster) expand.getCluster(),
                        ImmutableList.of(),
                        expand.getInput(0),
                        expand,
                        getV,
                        getV.getAliasName());

        return expandGetV;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GraphLogicalGetV getV = call.rel(0);
        GraphLogicalExpand expand = call.rel(1);
        GraphBuilder builder = (GraphBuilder) call.builder();
        call.transformTo(transform(getV, expand, builder));
    }

    public static class Config implements RelRule.Config {
        public static ExpandGetVFusionRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(GraphLogicalGetV.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(GraphLogicalExpand.class)
                                                                        .predicate(
                                                                                (GraphLogicalExpand
                                                                                                expand) -> {
                                                                                    int alias =
                                                                                            expand
                                                                                                    .getAliasId();
                                                                                    return alias
                                                                                            == AliasInference
                                                                                                    .DEFAULT_ID;
                                                                                })
                                                                        .anyInputs()));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new ExpandGetVFusionRule(this);
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
