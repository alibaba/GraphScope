package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.PushFilterVisitor;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;

public class FilterMatchRule<C extends FilterMatchRule.Config> extends RelRule<C>
        implements TransformationRule {
    protected FilterMatchRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        AbstractLogicalMatch match = call.rel(1);
        // do the transformation
        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        int origSize = conjunctions.size();
        GraphBuilder graphBuilder = (GraphBuilder) call.builder();
        for (Iterator<RexNode> it = conjunctions.iterator(); it.hasNext(); ) {
            RexNode condition = it.next();
            PushFilterVisitor visitor = new PushFilterVisitor(graphBuilder, condition);
            match = (AbstractLogicalMatch) match.accept(visitor);
            if (visitor.isPushed()) {
                it.remove();
            }
        }
        // condition in filter need to be updated
        if (!conjunctions.isEmpty() && conjunctions.size() < origSize) {
            filter = (Filter) graphBuilder.push(match).filter(conjunctions).build();
        }
        RelNode newNode = conjunctions.isEmpty() ? match : filter;
        call.transformTo(newNode);
    }

    public static class Config implements RelRule.Config {
        public static FilterMatchRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Filter.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                AbstractLogicalMatch
                                                                                        .class)
                                                                        .anyInputs()));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public RelRule toRule() {
            return new FilterMatchRule(this);
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
