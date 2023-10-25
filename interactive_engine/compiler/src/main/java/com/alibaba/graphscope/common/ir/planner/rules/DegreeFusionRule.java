package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpandDegree;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class DegreeFusionRule<C extends RelRule.Config> extends RelRule<C>
        implements TransformationRule {
    protected DegreeFusionRule(C config) {
        super(config);
    }

    protected RelNode transform(
            GraphLogicalAggregate count, GraphLogicalExpand expand, GraphBuilder builder) {
        RelNode expandDegree =
                GraphLogicalExpandDegree.create(
                        (GraphOptCluster) expand.getCluster(),
                        ImmutableList.of(),
                        expand.getInput(0),
                        expand,
                        null);
        builder.push(expandDegree);
        Preconditions.checkArgument(
                !count.getAggCalls().isEmpty(),
                "there should be at least one aggregate call in count");
        String countAlias = count.getAggCalls().get(0).getAlias();
        return builder.aggregate(
                        builder.groupKey(),
                        builder.sum0(false, countAlias, builder.variable((String) null)))
                .build();
    }

    // transform expand + count to expandDegree + sum
    public static class Expand extends DegreeFusionRule<Expand.Config> {
        protected Expand(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalAggregate count = call.rel(0);
            GraphLogicalExpand expand = call.rel(1);
            GraphBuilder builder = (GraphBuilder) call.builder();
            call.transformTo(transform(count, expand, builder));
        }

        public static class Config implements RelRule.Config {
            public static Expand.Config DEFAULT =
                    new Expand.Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalAggregate.class)
                                                    .predicate(
                                                            (GraphLogicalAggregate aggregate) -> {
                                                                RelBuilder.GroupKey key =
                                                                        aggregate.getGroupKey();
                                                                List<GraphAggCall> calls =
                                                                        aggregate.getAggCalls();
                                                                return key.groupKeyCount() == 0
                                                                        && calls.size() == 1
                                                                        && calls.get(0)
                                                                                        .getAggFunction()
                                                                                        .getKind()
                                                                                == SqlKind.COUNT
                                                                        && !calls.get(0)
                                                                                .isDistinct();
                                                            })
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(
                                                                                    GraphLogicalExpand
                                                                                            .class)
                                                                            .predicate(
                                                                                    (GraphLogicalExpand
                                                                                                    expand) ->
                                                                                            expand
                                                                                                            .getAliasName()
                                                                                                    == AliasInference
                                                                                                            .DEFAULT_NAME)
                                                                            .anyInputs()));
            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public Expand.Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public Expand.Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public Expand.Config withOperandSupplier(OperandTransform operandTransform) {
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
            public Expand toRule() {
                return new Expand(this);
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }

    // transform expand + getV + count to expandDegree + sum
    public static class ExpandGetV extends DegreeFusionRule<ExpandGetV.Config> {
        protected ExpandGetV(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalAggregate count = call.rel(0);
            GraphLogicalExpand expand = call.rel(2);
            GraphBuilder builder = (GraphBuilder) call.builder();
            call.transformTo(transform(count, expand, builder));
        }

        public static class Config implements RelRule.Config {
            public static ExpandGetV.Config DEFAULT =
                    new ExpandGetV.Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalAggregate.class)
                                                    // should be global count and is not distinct
                                                    .predicate(
                                                            (GraphLogicalAggregate aggregate) -> {
                                                                RelBuilder.GroupKey key =
                                                                        aggregate.getGroupKey();
                                                                List<GraphAggCall> calls =
                                                                        aggregate.getAggCalls();
                                                                return key.groupKeyCount() == 0
                                                                        && calls.size() == 1
                                                                        && calls.get(0)
                                                                                        .getAggFunction()
                                                                                        .getKind()
                                                                                == SqlKind.COUNT
                                                                        && !calls.get(0)
                                                                                .isDistinct();
                                                            })
                                                    .oneInput(
                                                            b1 ->
                                                                    // should be getV without any
                                                                    // query given alias, and opt is
                                                                    // not BOTH
                                                                    b1.operand(
                                                                                    GraphLogicalGetV
                                                                                            .class)
                                                                            .predicate(
                                                                                    (GraphLogicalGetV
                                                                                                    getV) ->
                                                                                            getV
                                                                                                                    .getAliasName()
                                                                                                            == AliasInference
                                                                                                                    .DEFAULT_NAME
                                                                                                    && getV
                                                                                                                    .getOpt()
                                                                                                            != GraphOpt
                                                                                                                    .GetV
                                                                                                                    .BOTH)
                                                                            .oneInput(
                                                                                    b2 ->
                                                                                            // should be expand without any query given alias
                                                                                            b2.operand(
                                                                                                            GraphLogicalExpand
                                                                                                                    .class)
                                                                                                    .predicate(
                                                                                                            (GraphLogicalExpand
                                                                                                                            expand) ->
                                                                                                                    expand
                                                                                                                                    .getAliasName()
                                                                                                                            == AliasInference
                                                                                                                                    .DEFAULT_NAME)
                                                                                                    .anyInputs())));
            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public ExpandGetV.Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public ExpandGetV.Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public ExpandGetV.Config withOperandSupplier(OperandTransform operandTransform) {
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
            public ExpandGetV toRule() {
                return new ExpandGetV(this);
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }
}
