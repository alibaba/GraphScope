package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasConverter;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
        List<RelNode> sentences = getSentences(match);
        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        int origSize = conjunctions.size();
        GraphBuilder graphBuilder = (GraphBuilder) call.builder();
        for (Iterator<RexNode> it = conjunctions.iterator(); it.hasNext(); ) {
            RexNode condition = it.next();
            if (pushFilter(sentences, condition, graphBuilder)) {
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

    private boolean pushFilter(
            List<RelNode> sentences, RexNode condition, GraphBuilder graphBuilder) {
        boolean pushed = false;
        List<Integer> distinctAliasIds =
                condition
                        .accept(new RexVariableAliasCollector<>(true, RexGraphVariable::getAliasId))
                        .stream()
                        .distinct()
                        .collect(Collectors.toList());
        if (distinctAliasIds.size() != 1) {
            return pushed;
        }
        int aliasId = distinctAliasIds.get(0);
        List<RelNode> inputsQueue = new ArrayList<>();
        for (RelNode node : sentences) {
            inputsQueue.add(node);
            while (!inputsQueue.isEmpty()) {
                RelNode cur = inputsQueue.remove(0);
                List<AbstractBindableTableScan> candidates = new ArrayList<>();
                if (cur instanceof AbstractBindableTableScan) {
                    candidates.add((AbstractBindableTableScan) cur);
                }
                for (AbstractBindableTableScan candidate : candidates) {
                    RelDataType rowType = candidate.getRowType();
                    for (RelDataTypeField field : rowType.getFieldList()) {
                        if (aliasId != AliasInference.DEFAULT_ID && field.getIndex() == aliasId) {
                            RexNode transform = condition.accept(new RexVariableAliasConverter(true, graphBuilder, AliasInference.DEFAULT_NAME, AliasInference.DEFAULT_ID));
                            if (ObjectUtils.isEmpty(candidate.getFilters())) {
                                candidate.setFilters(ImmutableList.of(transform));
                            } else {
                                ImmutableList.Builder builder = new ImmutableList.Builder();
                                builder.addAll(candidate.getFilters()).add(transform);
                                candidate.setFilters(
                                        ImmutableList.of(
                                                RexUtil.composeConjunction(
                                                        graphBuilder.getRexBuilder(),
                                                        builder.build())));
                            }
                            pushed = true;
                            break;
                        }
                    }
                }
                if (!cur.getInputs().isEmpty()) {
                    inputsQueue.addAll(cur.getInputs());
                }
            }
        }
        return pushed;
    }

    private List<RelNode> getSentences(AbstractLogicalMatch match) {
        List<RelNode> sentences = new ArrayList<>();
        if (match instanceof GraphLogicalSingleMatch) {
            sentences.add(((GraphLogicalSingleMatch) match).getSentence());
        } else if (match instanceof GraphLogicalMultiMatch) {
            sentences.addAll(((GraphLogicalMultiMatch) match).getSentences());
        } else {
            throw new UnsupportedOperationException(
                    "match type " + match.getClass() + " is unsupported");
        }
        return sentences;
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
