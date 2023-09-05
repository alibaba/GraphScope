package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GraphFieldTrimmer extends RelFieldTrimmer {
    private GraphBuilder graphBuilder;

    public class GraphVariable {
        private int aliasId;
        private @Nullable GraphProperty property;

        public GraphVariable(int aliasId, @Nullable GraphProperty property) {
            this.aliasId = aliasId;
            this.property = property;
        }

        public GraphVariable(int aliasId) {
            this.aliasId = aliasId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            RexGraphVariable rhs = (RexGraphVariable) o;
            if (rhs.getProperty() == null) {
                return aliasId == rhs.getAliasId();
            }
            return aliasId == rhs.getAliasId() && Objects.equals(property, rhs.getProperty());

        }
    }

    private final ReflectUtil.MethodDispatcher<RelNode> graphTrimFieldsDispatcher;

    public GraphFieldTrimmer(GraphBuilder builder) {
        super(null, builder);
        graphBuilder = builder;
        graphTrimFieldsDispatcher =
                ReflectUtil.createMethodDispatcher(
                        RelNode.class,
                        this,
                        "trimFields",
                        RelNode.class,
                        Set.class);


    }

    public RelNode trim(RelNode root) {
        final ImmutableSet<GraphVariable> fields = ImmutableSet.of();
        return dispatchTrimFields(root, fields);
    }


    public RelNode trimFields(GraphLogicalProject project, ImmutableSet<GraphVariable> fieldsUsed) {
        // TODO(huaiyu)
        return project;
    }

    public RelNode trimFields(GraphLogicalAggregate aggregate, ImmutableSet<GraphVariable> fieldsUsed) {
        ImmutableSet.Builder builder=ImmutableSet.builder();
        // collect aggregate calls
        List<GraphAggCall> calls=aggregate.getAggCalls();
        for(GraphAggCall call:calls){
            for(RexNode operand:call.getOperands()){
                operand.accept(new RexVariableAliasCollector<>(true, this::findField))
                        .stream()
                        .forEach(builder::add);
            }
        }

        // collect group bys
        GraphGroupKeys keys=aggregate.getGroupKey();
        for(RexNode node:keys.getVariables()){
            node.accept(new RexVariableAliasCollector<>(true, this::findField))
                    .stream()
                    .forEach(builder::add);
        }

        // TODO(huaiyu): convertor - convert

        fieldsUsed=builder.build();

        RelNode input = aggregate.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return aggregate;
        }
        graphBuilder.push(newInput).aggregate((RelBuilder.GroupKey) keys, (RelBuilder.AggCall) calls);
        return graphBuilder.build();
    }

    public RelNode trimFields(GraphLogicalSort sort, ImmutableSet<GraphVariable> fieldsUsed) {
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        RelCollation collation=sort.collation;


        ImmutableSet.Builder builder=ImmutableSet.builder();
        offset.accept(new RexVariableAliasCollector<>(true, this::findField))
                .stream()
                .forEach(builder::add);

        fetch.accept(new RexVariableAliasCollector<>(true, this::findField))
                .stream()
                .forEach(builder::add);
        // TODO(collect order by)

        builder.addAll(fieldsUsed);
        fieldsUsed=builder.build();

        RelNode input = sort.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return sort;
        }
        // FIXME(huaiyu) get nodes from collection
//        graphBuilder.push(newInput).sortLimit(offset,fetch)
        return graphBuilder.build();
    }


    public RelNode trimFields(LogicalFilter filter, ImmutableSet<GraphVariable> fieldsUsed) {
        // Find columns and PropertyRef used by filter.
        RexNode condition = filter.getCondition();

        ImmutableSet.Builder builder=ImmutableSet.builder();
        condition.accept(new RexVariableAliasCollector<>(true, this::findField))
                .stream()
                .forEach(builder::add);

        builder.addAll(fieldsUsed);
        fieldsUsed=builder.build();


        RelNode input = filter.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return filter;
        }

        graphBuilder.push(newInput).filter(filter.getVariablesSet(), filter.getCondition());
        return graphBuilder.build();
    }

    public RelNode trimFields(GraphLogicalSingleMatch singleMatch, ImmutableSet<GraphVariable> fieldsUsed) {
        List<RelNode> sentence = Collections.singletonList(singleMatch.getSentence());
        return singleMatch;
    }

    public RelNode trimFields(GraphLogicalMultiMatch multiMatch, ImmutableSet<GraphVariable> fieldUsed) {
        List<RelNode> setences = multiMatch.getSentences();
        List<RelNode> result = Collections.emptyList();
        for (RelNode node : setences) {
            trimChild(node, fieldUsed);
        }
        // TODO(huaiyu)
        return multiMatch;
    }

    public RelNode trimFields(AbstractBindableTableScan tableScan, ImmutableSet<GraphVariable> fieldsUsed) {
        // TODO(huaiyu)
        return tableScan;
    }


    protected RelNode trimChild(RelNode rel, ImmutableSet<GraphVariable> fieldsUsed) {
        return dispatchTrimFields(rel, fieldsUsed);

    }

    final public GraphVariable findField(RexGraphVariable var) {
        return new GraphVariable(var.getAliasId(), var.getProperty());
    }


    protected final RelNode dispatchTrimFields(
            RelNode rel,
            Set<GraphVariable> fieldsUsed
    ) {
        return graphTrimFieldsDispatcher.invoke(rel, fieldsUsed);
    }

}