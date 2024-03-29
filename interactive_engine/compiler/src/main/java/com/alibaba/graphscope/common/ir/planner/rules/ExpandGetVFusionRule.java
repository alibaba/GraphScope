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
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalGetV;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

// This rule try to fuse GraphLogicalExpand and GraphLogicalGetV if GraphLogicalExpand has no alias
// (that it won't be visited individually later):
// 1. if GraphLogicalGetV has no filters, then:
//      GraphLogicalExpand + GraphLogicalGetV -> GraphPhysicalExpand(ExpandV),
// where GraphPhysicalExpand carries the alias of GraphLogicalGetV
// 2. if GraphLogicalGetV has filters, then:
//      GraphLogicalExpand + GraphLogicalGetV -> GraphPhysicalExpand(ExpandV)  +
// GraphPhysicalGetV(VertexFilter),
// where GraphPhysicalExpand carries the Default alias, and GraphPhysicalGetV carries the alias of
// GraphLogicalGetV
public abstract class ExpandGetVFusionRule<C extends RelRule.Config> extends RelRule<C>
        implements TransformationRule {

    protected ExpandGetVFusionRule(C config) {
        super(config);
    }

    protected RelNode transform(GraphLogicalGetV getV, GraphLogicalExpand expand, RelNode input) {
        if (expand.getOpt().equals(GraphOpt.Expand.OUT) && getV.getOpt().equals(GraphOpt.GetV.END)
                || expand.getOpt().equals(GraphOpt.Expand.IN)
                        && getV.getOpt().equals(GraphOpt.GetV.START)
                || expand.getOpt().equals(GraphOpt.Expand.BOTH)
                        && getV.getOpt().equals(GraphOpt.GetV.OTHER)) {
            if (ObjectUtils.isEmpty(getV.getFilters())) {
                GraphPhysicalExpand physicalExpand =
                        GraphPhysicalExpand.create(
                                expand.getCluster(),
                                expand.getHints(),
                                input,
                                expand,
                                getV,
                                GraphOpt.PhysicalExpandOpt.VERTEX,
                                getV.getAliasName());
                return physicalExpand;
            } else {
                GraphPhysicalExpand physicalExpand =
                        GraphPhysicalExpand.create(
                                expand.getCluster(),
                                expand.getHints(),
                                input,
                                expand,
                                getV,
                                GraphOpt.PhysicalExpandOpt.VERTEX,
                                AliasInference.DEFAULT_NAME);
                // If with filters, then create a GraphPhysicalGetV to do the filtering.
                // We set alias of getV to null to avoid alias conflict (with expand's alias)
                GraphPhysicalGetV physicalGetV =
                        GraphPhysicalGetV.create(
                                getV.getCluster(),
                                getV.getHints(),
                                physicalExpand,
                                getV,
                                getV.getAliasName(),
                                GraphOpt.PhysicalGetVOpt.ITSELF);
                return physicalGetV;
            }
        } else {
            return getV;
        }
    }

    // transform expande + getv to GraphPhysicalExpandGetV
    public static class BasicExpandGetVFusionRule
            extends ExpandGetVFusionRule<BasicExpandGetVFusionRule.Config> {
        protected BasicExpandGetVFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalGetV getV = call.rel(0);
            GraphLogicalExpand expand = call.rel(1);
            call.transformTo(transform(getV, expand, expand.getInput(0)));
        }

        public static class Config implements RelRule.Config {
            public static BasicExpandGetVFusionRule.Config DEFAULT =
                    new Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalGetV.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(
                                                                                    GraphLogicalExpand
                                                                                            .class)
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
                return new BasicExpandGetVFusionRule(this);
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

    // transform expande + getv to GraphPhysicalExpandGetV in PathExpand
    public static class PathBaseExpandGetVFusionRule
            extends ExpandGetVFusionRule<PathBaseExpandGetVFusionRule.Config> {
        protected PathBaseExpandGetVFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalPathExpand pathExpand = call.rel(0);
            GraphLogicalGetV getV = (GraphLogicalGetV) pathExpand.getGetV();
            GraphLogicalExpand expand = (GraphLogicalExpand) pathExpand.getExpand();
            RelNode fused = transform(getV, expand, null);
            GraphLogicalPathExpand afterPathExpand =
                    GraphLogicalPathExpand.create(
                            (GraphOptCluster) pathExpand.getCluster(),
                            ImmutableList.of(),
                            pathExpand.getInput(0),
                            fused,
                            pathExpand.getOffset(),
                            pathExpand.getFetch(),
                            pathExpand.getResultOpt(),
                            pathExpand.getPathOpt(),
                            pathExpand.getAliasName(),
                            pathExpand.getStartAlias());
            call.transformTo(afterPathExpand);
        }

        public static class Config implements RelRule.Config {
            public static PathBaseExpandGetVFusionRule.Config DEFAULT =
                    new Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalPathExpand.class)
                                                    .predicate(
                                                            (GraphLogicalPathExpand pathExpand) -> {
                                                                if (GraphOpt.PathExpandResult
                                                                        .ALL_V_E
                                                                        .equals(
                                                                                pathExpand
                                                                                        .getResultOpt())) {
                                                                    return false;
                                                                } else {
                                                                    if (pathExpand.getExpand()
                                                                            instanceof
                                                                            GraphLogicalExpand) {
                                                                        GraphLogicalExpand expand =
                                                                                (GraphLogicalExpand)
                                                                                        pathExpand
                                                                                                .getExpand();
                                                                        int alias =
                                                                                expand.getAliasId();
                                                                        return alias
                                                                                == AliasInference
                                                                                        .DEFAULT_ID;
                                                                    } else {
                                                                        return false;
                                                                    }
                                                                }
                                                            })
                                                    .anyInputs());

            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public RelRule toRule() {
                return new PathBaseExpandGetVFusionRule(this);
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
}
