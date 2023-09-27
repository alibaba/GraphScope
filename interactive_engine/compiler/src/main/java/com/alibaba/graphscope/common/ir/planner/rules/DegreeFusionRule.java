package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasConverter;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import org.apache.calcite.sql.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class DegreeFusionRule<C extends DegreeFusionRule.Config> extends RelRule<C>
        implements TransformationRule {
    protected DegreeFusionRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GraphLogicalAggregate aggregate = call.rel(0);
        GraphLogicalGetV getv = call.rel(1);
        GraphLogicalExpand expand = call.rel(2);
        
        GraphBuilder graphBuilder = (GraphBuilder)call.builder();

        List<GraphAggCall> groupCalls = aggregate.getAggCalls();
        if (!(groupCalls.size() == 1
                && aggregate.getGroupKey().groupKeyCount() == 0)) {// only one, and is COUNT
            return ;
        }

        // Create new expandcount for Degree Fusion
        RelNode expandCount =
                GraphLogicalExpandCount.create(
                        (GraphOptCluster) expand.getCluster(),
                        ImmutableList.of(),
                        expand.getInput(0),
                        (GraphLogicalExpand) expand,
                        expand.getAliasName());

        List<GraphAggCall> newCalls = new ArrayList<>(aggregate.getAggCallList().size());
        for (GraphAggCall aggregateCall: groupCalls) {
            GraphAggCall newCall = new GraphAggCall(
                    aggregateCall.getCluster(),
                    GraphStdOperatorTable.SUM, // change
                    aggregateCall.getOperands()) // change?
                    .as(aggregateCall.getAlias())
                    .distinct(aggregateCall.isDistinct());
            newCalls.add(newCall);
        }

        // expandcount -> aggregate(sum)
        GraphLogicalAggregate newAggregate = GraphLogicalAggregate.create(
                (GraphOptCluster) expandCount.getCluster(),
                ImmutableList.of(),
                expandCount,
                aggregate.getGroupKey(), // is empty
                newCalls);
        
        call.transformTo(newAggregate);
    }

    public static class Config implements RelRule.Config {
        public static DegreeFusionRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 -> b0.operand(GraphLogicalAggregate.class)
                                        .predicate(aggregate -> aggregate.getGroupKey()
                                                .groupKeyCount() == 0)
//                                        .predicate(aggregate -> aggregate.getAggCalls().get(0).
//                                                                getAggFunction() == SqlKind.COUNT)
                                        .oneInput(b1 -> b1.operand(GraphLogicalGetV.class)
                                                .oneInput(b2 -> b2.operand(GraphLogicalExpand.class)
                                                        .anyInputs())
                                        )
                        )
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