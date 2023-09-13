package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GetVConfig;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ReflectUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class GraphFieldTrimmer extends RelFieldTrimmer {
    private final ReflectUtil.MethodDispatcher<RelNode> graphTrimFieldsDispatcher;
    private final GraphBuilder graphBuilder;

    public GraphFieldTrimmer(GraphBuilder builder) {
        super(null, builder);
        graphBuilder = builder;
        graphTrimFieldsDispatcher = ReflectUtil.createMethodDispatcher(RelNode.class, this, "trimFields", RelNode.class, Set.class);
    }

    public RelNode trim(RelNode root) {
        final ImmutableSet<GraphVariable> fields = ImmutableSet.of();
        return dispatchTrimFields(root, fields);
    }

    // TODO( we need another data structure in project to record used properties(of current columns)
    public RelNode trimFields(GraphLogicalProject project, ImmutableSet<GraphVariable> fieldsUsed) {
        ImmutableSet.Builder fieldUsedBuilder = ImmutableSet.builder();
        List<RelDataTypeField> fieldList = project.getRowType().getFieldList();
        List<RexNode> usedProjects = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
            RelDataTypeField field = fieldList.get(ord.i);
            GraphVariable var = new GraphVariable(field.getIndex(), null);
            if (fieldsUsed.contains(var)) {
                List<GraphVariable> list = ord.e.accept(new RexVariableAliasCollector<>(true, this::findField)).stream().collect(Collectors.toUnmodifiableList());

                //convert: v as a :  a.id=>v.id
                if (list.size() == 1) {
                    GraphVariable origin = list.get(0);
                    if (origin.property == null) {
                        fieldsUsed.stream().filter((e) -> e.property != null && e.aliasId == field.getIndex()).map((e) -> new GraphVariable(origin.aliasId, e.property)).forEach(fieldUsedBuilder::add);
                    }
                }
                fieldUsedBuilder.addAll(list);
                usedProjects.add(ord.e);
                alias.add(field.getName());
            }
        }


        //  check if current field doesn't appear in project's rowType. If not, add it
        if (project.isAppend()) {
            ImmutableBitSet idxSet = fieldList.stream().map((e) -> e.getIndex()).collect(ImmutableBitSet.toImmutableBitSet());

            fieldList.stream().filter((e) -> !idxSet.get(e.getIndex())).forEach(fieldUsedBuilder::add);

        }


        RelNode input = project.getInput();
        RelNode newInput = trimChild(input, fieldUsedBuilder.build());
        if (Objects.equals(input, newInput) && usedProjects.size() == project.getProjects().size()) {
            return project;
        }
        graphBuilder.push(newInput).project(usedProjects, alias);
        return graphBuilder.build();
    }

    public RelNode trimFields(GraphLogicalAggregate aggregate, ImmutableSet<GraphVariable> fieldsUsed) {
        // TODO(huaiyu): need to remove used aggregate function?


        ImmutableSet.Builder fieldUsedBuilder = ImmutableSet.builder();

        // for aggregate calls, only record the graph variable used by calls
        List<GraphAggCall> calls = aggregate.getAggCalls();
        for (GraphAggCall call : calls) {
            for (RexNode operand : call.getOperands()) {
                operand.accept(new RexVariableAliasCollector<>(true, this::findField))
                        .stream().forEach(fieldUsedBuilder::add);
            }
        }

        // for group by keys, do we need to collect and convert?
        //FIXME(huaiyu) e.g:  group().by(values("v").as("a")) where v is a node?
        GraphGroupKeys keys = aggregate.getGroupKey();
        for (RexNode node : keys.getVariables()) {
            node.accept(new RexVariableAliasCollector<>(true, this::findField)).stream().forEach(fieldUsedBuilder::add);
        }


        fieldsUsed = fieldUsedBuilder.build();

        RelNode input = aggregate.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return aggregate;
        }
        graphBuilder.push(newInput).aggregate(keys, (RelBuilder.AggCall) calls);
        return graphBuilder.build();
    }

    public RelNode trimFields(GraphLogicalSort sort, ImmutableSet<GraphVariable> fieldsUsed) {
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        List<RelFieldCollation> collation = sort.collation.getFieldCollations();


        ImmutableSet.Builder builder = ImmutableSet.builder();
        offset.accept(new RexVariableAliasCollector<>(true, this::findField)).stream().forEach(builder::add);

        fetch.accept(new RexVariableAliasCollector<>(true, this::findField)).stream().forEach(builder::add);

        // TODO(huaiyu) is that correct?
        for (RelFieldCollation rfc : collation) {
            builder.add(new GraphVariable(rfc.getFieldIndex(), null));
        }

        builder.addAll(fieldsUsed);
        fieldsUsed = builder.build();

        RelNode input = sort.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return sort;
        }

        graphBuilder.push(newInput).sortLimit(offset, fetch, sort.getSortExps());
        return graphBuilder.build();
    }

    public RelNode trimFields(LogicalFilter filter, ImmutableSet<GraphVariable> fieldsUsed) {
        // Find columns and PropertyRef used by filter.
        RexNode condition = filter.getCondition();

        ImmutableSet.Builder fieldUsedBuilder = ImmutableSet.builder();
        condition.accept(new RexVariableAliasCollector<>(true, this::findField)).stream().forEach(fieldUsedBuilder::add);

        fieldUsedBuilder.addAll(fieldsUsed);
        fieldsUsed = fieldUsedBuilder.build();


        RelNode input = filter.getInput();
        RelNode newInput = trimChild(input, fieldsUsed);
        if (Objects.equals(input, newInput)) {
            return filter;
        }

        graphBuilder.push(newInput).filter(filter.getVariablesSet(), filter.getCondition());
        return graphBuilder.build();
    }

    public RelNode trimFields(GraphLogicalSingleMatch singleMatch, ImmutableSet<GraphVariable> fieldsUsed) {
        RelNode sentence = singleMatch.getSentence();
        RelNode result = trimChild(sentence, fieldsUsed);

        if (Objects.equals(sentence, result)) {
            return singleMatch;
        }

        return graphBuilder.match(sentence, singleMatch.getMatchOpt()).build();
    }

    public RelNode trimFields(GraphLogicalMultiMatch multiMatch, ImmutableSet<GraphVariable> fieldUsed) {
        List<RelNode> sentences = multiMatch.getSentences();
        List<RelNode> result = Collections.emptyList();
        boolean changed=false;
        for (RelNode node : sentences) {
            RelNode newInput=trimChild(node, fieldUsed);
            result.add(newInput);
            if(!changed&&Objects.equals(newInput,node)){
                changed=true;
            }
        }
        if(!changed){
            return multiMatch;
        }

        return graphBuilder.match(result.get(0),result.subList(1,sentences.size())).build();
    }

    public RelNode trimFields(AbstractBindableTableScan tableScan, ImmutableSet<GraphVariable> fieldsUsed) {
        ImmutableSet.Builder fieldUsedBuilder = ImmutableSet.builder();
        int aliasId=tableScan.getAliasId();
        fieldsUsed.stream().filter(e->e.property!=null&&e.aliasId==aliasId).forEach(fieldUsedBuilder::add);
        Set<GraphVariable> currentFieldUsed=fieldUsedBuilder.build();
        LabelConfig labelConfig=new LabelConfig(false);
        tableScan.getTableConfig().getTables().stream().map(e->e.getQualifiedName().get(0))
                .forEach(labelConfig::addLabel);

        if(tableScan instanceof GraphLogicalSource){
            GraphLogicalSource source= (GraphLogicalSource) tableScan;
            SourceConfig config=new SourceConfig(source.getOpt(),labelConfig,
                    currentFieldUsed.isEmpty()?null:source.getAliasName());
            return graphBuilder.source(config).build();

        }else if(tableScan instanceof GraphLogicalExpand){
            GraphLogicalExpand expand= (GraphLogicalExpand) tableScan;
            RelNode input=expand.getInput(0);
            RelNode newInput=trimChild(input,fieldsUsed);
            ExpandConfig config=new ExpandConfig(expand.getOpt(),labelConfig,currentFieldUsed.isEmpty()?null:expand.getAliasName());
            return graphBuilder.push(newInput).expand(config).build();

        }else if(tableScan instanceof GraphLogicalGetV){
            GraphLogicalGetV getv=(GraphLogicalGetV) tableScan;
            RelNode input=getv.getInput(0);
            RelNode newInput=trimChild(input,fieldsUsed);
            GetVConfig config=new GetVConfig(getv.getOpt(),labelConfig,currentFieldUsed.isEmpty()?null:getv.getAliasName());
            return graphBuilder.push(newInput).getV(config).build();
        }

        return tableScan;
    }

    protected RelNode trimChild(RelNode rel, ImmutableSet<GraphVariable> fieldsUsed) {
        return dispatchTrimFields(rel, fieldsUsed);

    }

    final public GraphVariable findField(RexGraphVariable var) {
        return new GraphVariable(var.getAliasId(), var.getProperty());
    }

    protected final RelNode dispatchTrimFields(RelNode rel, Set<GraphVariable> fieldsUsed) {
        return graphTrimFieldsDispatcher.invoke(rel, fieldsUsed);
    }

    public class GraphVariable {
        private final int aliasId;
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
}