package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpandCount;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class DegreeFusionRule<C extends DegreeFusionRule.Config> extends RelRule<C>
        implements TransformationRule {
    protected DegreeFusionRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GraphLogicalAggregate aggregate = call.rel(0);
        GraphLogicalGetV getV = call.rel(1);
        GraphLogicalExpand expand = call.rel(2);

        GraphBuilder graphBuilder = (GraphBuilder) call.builder();

        List<GraphAggCall> groupCalls = aggregate.getAggCalls();

        // aggregate have one aggcall, and none key, which means this is COUNT(*)
        if (!(groupCalls.size() == 1 && aggregate.getGroupKey().groupKeyCount() == 0)) {
            return;
        }

        // if expand has alias, e.g. g.V().out().as("a"),
        // or getV has alias, e.g. g.V().as("a").out(),
        // can't fusion
        //        if (expand.getAliasName() != null
        //                || expand.getAliasName() != AliasInference.DEFAULT_NAME
        //                || getV.getAliasName() != null
        //                || getV.getAliasName() != AliasInference.DEFAULT_NAME) {
        //            return;
        //        }

        // Create new expandcount for Degree Fusion
        // type is RecordType(BIGINT cnt of y)
        RelNode expandCount =
                GraphLogicalExpandCount.create(
                        (GraphOptCluster) expand.getCluster(),
                        ImmutableList.of(),
                        expand.getInput(0),
                        (GraphLogicalExpand) expand,
                        "cnt of " + expand.getAliasName());
        graphBuilder.push(expandCount);

        //        GroupKey key = graphBuilder.groupKey();

        // create new aggcalls for new aggregate
        List<GraphAggCall> newCalls = new ArrayList<>(aggregate.getAggCallList().size());
        int i = 1; // for test
        for (GraphAggCall aggregateCall : groupCalls) {
            System.out.println(i);
            i++; // for test
            // get first operand, which is expandCount
            List<RexNode> expandCountNode = new ArrayList<>();
            expandCountNode.add(
                    graphBuilder.variable(((GraphLogicalExpandCount) expandCount).getAliasName()));
            GraphAggCall newCall =
                    new GraphAggCall(
                                    // aggregateCall.getCluster(),
                                    (GraphOptCluster) expandCount.getCluster(),
                                    GraphStdOperatorTable.SUM, // change
                                    expandCountNode)
                            .as("sum")
                            .distinct(aggregateCall.isDistinct());
            newCalls.add(newCall);
        }

        // create aggregate(sum)
        GraphLogicalAggregate newAggregate =
                GraphLogicalAggregate.create(
                        (GraphOptCluster) expandCount.getCluster(),
                        ImmutableList.of(),
                        expandCount,
                        (GraphGroupKeys) graphBuilder.groupKey(), // SUM, also empty
                        newCalls);

        System.out.println(newAggregate.getRowType().toString());

        call.transformTo(newAggregate);
    }

    public static class Config implements RelRule.Config {
        public static DegreeFusionRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(GraphLogicalAggregate.class)
                                                .predicate(
                                                        aggregate ->
                                                                aggregate
                                                                                .getGroupKey()
                                                                                .groupKeyCount()
                                                                        == 0)
                                                //
                                                // .predicate(aggregate ->
                                                // aggregate.getAggCalls().get(0).
                                                //
                                                //              getAggFunction() == SqlKind.COUNT)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(GraphLogicalGetV.class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        GraphLogicalExpand
                                                                                                                .class)
                                                                                                .anyInputs())))
                        .withRelBuilderFactory(
                                (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                                        GraphBuilder.create(
                                                null, (GraphOptCluster) cluster, schema));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new DegreeFusionRule(this);
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
