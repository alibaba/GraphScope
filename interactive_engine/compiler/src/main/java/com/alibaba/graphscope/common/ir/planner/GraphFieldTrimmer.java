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
import com.alibaba.graphscope.common.ir.rex.RexPermuteGraphShuttle;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GetVConfig;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class GraphFieldTrimmer extends RelFieldTrimmer {
    private final ReflectUtil.MethodDispatcher<TrimResult> graphTrimFieldsDispatcher;
    private final GraphBuilder graphBuilder;

    public GraphFieldTrimmer(GraphBuilder builder) {
        super(null, builder);
        graphBuilder = builder;
        graphTrimFieldsDispatcher = ReflectUtil.createMethodDispatcher(TrimResult.class, this, "trimFields", RelNode.class,
                Map.class);
    }

    public RelNode trim(RelNode root) {

        ImmutableSet<RelDataTypeField> fields = findUsedField(root);
        Map<Integer, RelDataTypeField> fieldsUsed = new HashMap<>();
        for (RelDataTypeField field : fields) {
            fieldsUsed.put(field.getIndex(), field);
        }
        return dispatchTrimFields(root, fieldsUsed).left;
    }


    public TrimResult trimFields(GraphLogicalProject project, ImmutableMap<Integer, RelDataTypeField> fieldsUsed) {
        final RelDataType rowType = project.getRowType();
        final RelDataType inputRowType = project.getInput()
                                                .getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        // key: id of field in project RowType, value: id of filed in input RowType
        Map<Integer, Integer> aliasMap = new HashMap<>();
        final int fieldCount = rowType.getFieldCount();

        Map<Integer, RelDataTypeField> inputFieldUsed = new HashMap<>();
        final Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldsUsed.size());

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        List<RexNode> newProjects = new ArrayList<>();
        List<RelDataTypeField> newFieldList = new ArrayList<>();


        for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
            RelDataTypeField field = fieldList.get(ord.i);
            RexNode proj = ord.e;

            // If parents doesn't use current field, just trim it
            if (!fieldsUsed.keySet()
                           .contains(field.getIndex())) {
                continue;
            }

            mapping.set(ord.i, newProjects.size());
            newProjects.add(proj);


            // find field used by project
            List<RexGraphVariable> list = ord.e.accept(new RexVariableAliasCollector<>(true, this::findInput))
                                               .stream()
                                               .collect(Collectors.toUnmodifiableList());

            // if output type is  node/edge, we can simply think this proj just do alias
            if (list.size() == 1 && field.getType() instanceof GraphSchemaType) {
                RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
                RexGraphVariable var = list.get(0);

                //e.g `with v as person`, need to convert person.name back to v.name
                inputFieldUsed.put(var.getAliasId(), new RelDataTypeFieldImpl(var.getName(), var.getAliasId(),
                        parentsUsedField.getType()));

                // add parent used field as new filed to apply property trimming
                newFieldList.add(parentsUsedField);
            } else {
                newFieldList.add(field);
            }
            varUsedBuilder.addAll(list);
        }

        // If project is append, we:
        // 1. Check whether the field is used by parents
        // 2. If used, create a new RexGraphVariable as project item, add it in inputFieldUsed and newFieldList
        int i = project.getProjects()
                       .size();
        for (; i < fieldList.size(); ++i) {
            RelDataTypeField field = fieldList.get(i);
            if (!fieldsUsed.keySet()
                           .contains(field.getIndex())) {
                continue;
            }
            RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
            mapping.set(i, newProjects.size());
            RexGraphVariable proj = RexGraphVariable.of(field.getIndex(), i, field.getName(), field.getType());
            newProjects.add(proj);
            inputFieldUsed.put(parentsUsedField.getIndex(), parentsUsedField);
            newFieldList.add(parentsUsedField);
        }

        // e.g: with v as person, v.age as age where age>1 and person.name <> "Li"
        // need concat inputFieldUsed(v.name) and currentFields(v.age)
        ImmutableSet<RelDataTypeField> currentFields =
                compoundFields(varUsedBuilder.build(), inputRowType.getFieldList());

        for (RelDataTypeField field : currentFields) {
            if (inputFieldUsed.containsKey(field.getIndex())) {
                RelDataTypeField used = inputFieldUsed.get(field.getIndex());
                GraphSchemaType newType = concatGraphFields((GraphSchemaType) field.getType(),
                        (GraphSchemaType) used.getType());
                inputFieldUsed.put(field.getIndex(), new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType));
            } else {
                inputFieldUsed.put(field.getIndex(), field);
            }
        }

        //trim child
        RelNode input = project.getInput();
        TrimResult trimResult = trimChild(input, inputFieldUsed);
        RelNode newInput = trimResult.left;


        final Mapping inputMapping = trimResult.right;

        if (newFieldList.size() == 0) {
            return dummyProject(fieldCount, newInput, project);
        }

        // build new projects
        final RexVisitor<RexNode> shuttle =
                new RexPermuteGraphShuttle(
                        inputMapping, newInput);

        // build new RowType


        RelRecordType newRowType = new RelRecordType(StructKind.FULLY_QUALIFIED, newFieldList);

        graphBuilder.push(newInput)
                    .project(newProjects.stream()
                                        .map(e -> e.accept(shuttle))
                                        .collect(Collectors.toList()), newRowType);

        final RelNode newProject = graphBuilder.build();
        return result(newProject, mapping, project);
    }

    public TrimResult trimFields(GraphLogicalAggregate aggregate, Map<Integer, RelDataTypeField> fieldsUsed) {

        ArrayList<GraphAggCall> newCalls = new ArrayList<>();
        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        Map<Integer, RelDataTypeField> inputFieldUsed = new HashMap<>();
        int keySize = aggregate.getGroupKey()
                               .groupKeyCount();

        final RelDataType rowType = aggregate.getRowType();
        final RelDataType inputRowType = aggregate.getInput()
                                                  .getRowType();
        final int fieldCount = rowType.getFieldCount();
        final Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldsUsed.size());


        // for group by keys, do we need to collect and convert?
        // e.g:  group().by(values("v").as("a")) where v is a node?
        GraphGroupKeys keys = aggregate.getGroupKey();
        for (Ord<RexNode> ord : Ord.zip(keys.getVariables())) {
            RexNode node = ord.e;
            List<RexGraphVariable> vars = node.accept(new RexVariableAliasCollector<>(true, this::findInput))
                                              .stream()
                                              .collect(Collectors.toUnmodifiableList());

            mapping.set(ord.i, ord.i);
            RelDataTypeField field = rowType.getFieldList()
                                            .get(ord.i);
            // we think it's just an alias
            if (vars.size() == 1 && field.getType() instanceof GraphSchemaType) {
                RexGraphVariable var = vars.get(0);
                if (fieldsUsed.containsKey(field.getIndex())) {
                    RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
                    inputFieldUsed.put(var.getAliasId(), new RelDataTypeFieldImpl(var.getName(), var.getAliasId(),
                            parentsUsedField.getType()));
                } else {
                    GraphSchemaType origin = (GraphSchemaType) field.getType();
                    GraphSchemaType graphSchemaType = new GraphSchemaType(origin.getScanOpt(), origin.getLabelType(),
                            List.of());
                    inputFieldUsed.put(var.getAliasId(), new RelDataTypeFieldImpl(var.getName(), var.getAliasId(),
                            graphSchemaType));
                }
            }
            varUsedBuilder.addAll(vars);
        }


        // for aggregate calls, only record the graph variable used by calls
        for (Ord<GraphAggCall> ord : Ord.zip(aggregate.getAggCalls())) {
            GraphAggCall call = ord.e;
            RelDataTypeField field = rowType.getFieldList()
                                            .get(ord.i);
            if (fieldsUsed.containsKey(field.getIndex())) {
                for (RexNode operand : call.getOperands()) {
                    operand.accept(new RexVariableAliasCollector<>(true, this::findInput))
                           .stream()
                           .forEach(varUsedBuilder::add);
                }
                mapping.set(ord.i + keySize, newCalls.size() + keySize);
                newCalls.add(call);
            }
        }

        // combine parents used fields and current used fields;
        ImmutableSet<RelDataTypeField> currentFields = compoundFields(varUsedBuilder.build(), inputRowType.getFieldList());
        for (RelDataTypeField field : currentFields) {
            if (inputFieldUsed.containsKey(field.getIndex())) {
                RelDataTypeField used = inputFieldUsed.get(field.getIndex());
                GraphSchemaType newType = concatGraphFields((GraphSchemaType) field.getType(),
                        (GraphSchemaType) used.getType());
                inputFieldUsed.put(field.getIndex(), new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType));
            } else {
                inputFieldUsed.put(field.getIndex(), field);
            }
        }

        // trim child
        RelNode input = aggregate.getInput();
        TrimResult result = trimChild(input, fieldsUsed);
        RelNode newInput = result.left;
        Mapping inputMapping = result.right;

        //TODO(huaiyu): generate new aggregate
        graphBuilder.push(newInput)
                    .aggregate(keys, (RelBuilder.AggCall) newCalls);
        RelNode newAggregate = graphBuilder.build();
        return result(newAggregate, mapping, aggregate);
    }

    public TrimResult trimFields(GraphLogicalSort sort, Map<Integer, RelDataTypeField> fieldsUsed) {
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        RelNode input = sort.getInput();
        final RelDataType inputRowType = input.getRowType();
        List<RelFieldCollation> collation = sort.collation.getFieldCollations();


        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        offset.accept(new RexVariableAliasCollector<>(true, this::findInput))
              .stream()
              .forEach(varUsedBuilder::add);

        fetch.accept(new RexVariableAliasCollector<>(true, this::findInput))
             .stream()
             .forEach(varUsedBuilder::add);
        for (RexNode expr : sort.getSortExps()) {
            expr.accept(new RexVariableAliasCollector<>(true, this::findInput))
                .stream()
                .forEach(varUsedBuilder::add);
        }


        ImmutableSet<RelDataTypeField> current = compoundFields(varUsedBuilder.build(), inputRowType.getFieldList());

        for (RelDataTypeField field : current) {
            if (field.getType() instanceof GraphSchemaType && fieldsUsed.containsKey(field.getIndex())) {
                RelDataTypeField used = fieldsUsed.get(field.getIndex());
                GraphSchemaType newType = concatGraphFields((GraphSchemaType) used.getType(),
                        (GraphSchemaType) field.getType());
                fieldsUsed.put(field.getIndex(), new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType));
            } else {
                fieldsUsed.put(field.getIndex(), field);
            }
        }


        TrimResult trimResult = trimChild(input, fieldsUsed);
        RelNode newInput = trimResult.left;
        Mapping inputMapping = trimResult.right;
        final RexVisitor<RexNode> shuttle =
                new RexPermuteGraphShuttle(
                        inputMapping, newInput);
        RexNode newOffset = sort.offset.accept(shuttle);
        RexNode newFetch = sort.offset.accept(shuttle);
        List<RexNode> newSortExprs =
                sort.getSortExps()
                    .stream()
                    .map(e -> e.accept(shuttle))
                    .collect(Collectors.toUnmodifiableList());

        graphBuilder.push(newInput)
                    .sortLimit(newOffset, newFetch, newSortExprs);
        RelNode newSort = graphBuilder.build();
        return result(newSort, inputMapping, sort);
    }

    public TrimResult trimFields(LogicalFilter filter, Map<Integer, RelDataTypeField> fieldsUsed) {
        RelDataType inputRowType = filter.getInput()
                                         .getRowType();
        Map<Integer, RelDataTypeField> inputFieldsUsed = new HashMap<>(fieldsUsed);
        // Find columns and PropertyRef used by filter.
        RexNode condition = filter.getCondition();

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        condition.accept(new RexVariableAliasCollector<>(true, this::findInput))
                 .stream()
                 .forEach(varUsedBuilder::add);
        ImmutableSet<RelDataTypeField> current = compoundFields(varUsedBuilder.build(), inputRowType.getFieldList());

        for (RelDataTypeField field : current) {
            if (field.getType() instanceof GraphSchemaType && fieldsUsed.containsKey(field.getIndex())) {
                RelDataTypeField used = fieldsUsed.get(field.getIndex());
                GraphSchemaType newType = concatGraphFields((GraphSchemaType) used.getType(),
                        (GraphSchemaType) field.getType());
                fieldsUsed.put(field.getIndex(), new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType));
            } else {
                fieldsUsed.put(field.getIndex(), field);
            }
        }

        // Trim child
        RelNode input = filter.getInput();
        TrimResult trimResult = trimChild(input, fieldsUsed);
        RelNode newInput = trimResult.left;
        Mapping inputMapping = trimResult.right;


        if (Objects.equals(input, newInput)) {
            return result(filter, inputMapping);
        }

        // use inputMapping shuttle conditions
        final RexVisitor<RexNode> shuttle =
                new RexPermuteGraphShuttle(
                        inputMapping, newInput);
        RexNode newCondition = condition.accept(shuttle);

        graphBuilder.push(newInput)
                    .filter(filter.getVariablesSet(), newCondition);
        RelNode newFilter = graphBuilder.build();
        return result(newFilter, inputMapping, filter);
    }

    public TrimResult trimFields(GraphLogicalSingleMatch singleMatch, Map<Integer, RelDataTypeField> fieldsUsed) {
        RelNode sentence = singleMatch.getSentence();
        TrimResult result = trimChild(sentence, fieldsUsed);
        RelNode newInput = result.left;
        Mapping inputMapping = result.right;

        if (Objects.equals(sentence, result)) {
            return result(singleMatch, inputMapping);
        }

        RelNode newMatch = graphBuilder.match(newInput, singleMatch.getMatchOpt())
                                       .build();
        return result(newMatch, inputMapping, singleMatch);
    }

    public TrimResult trimFields(GraphLogicalMultiMatch multiMatch, Map<Integer, RelDataTypeField> fieldsUsed) {
        List<RelNode> sentences = multiMatch.getSentences();
        List<RelNode> newInputs = Collections.emptyList();
        int fieldCount = multiMatch.getRowType()
                                   .getFieldCount();
        boolean changed = false;
        for (RelNode node : sentences) {
            TrimResult result = trimChild(node, fieldsUsed);
            newInputs.add(result.left);
            if (!changed && Objects.equals(result.left, node)) {
                changed = true;
            }
        }
        Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        RelNode newMatch = graphBuilder.match(newInputs.get(0), newInputs.subList(1, sentences.size()))
                                       .build();
        return result(newMatch, mapping, multiMatch);
    }

    public TrimResult trimFields(AbstractBindableTableScan tableScan, Map<Integer, RelDataTypeField> fieldsUsed) {
        RelDataType rowType = tableScan.getRowType();
        int aliasId = tableScan.getAliasId();
        int fieldCount = rowType.getFieldCount();
        LabelConfig labelConfig = new LabelConfig(false);
        tableScan.getTableConfig()
                 .getTables()
                 .stream()
                 .map(e -> e.getQualifiedName()
                            .get(0))
                 .forEach(labelConfig::addLabel);

        // create new RowType of table scan
        RelDataTypeField field;
        if (fieldsUsed.containsKey(aliasId)) {
            field = fieldsUsed.get(aliasId);
        } else {
            GraphSchemaType origin = (GraphSchemaType) rowType.getFieldList()
                                                              .get(0);
            GraphSchemaType newType = new GraphSchemaType(origin.getScanOpt(), origin.getLabelType(), new ArrayList<>()
                    , origin.isNullable());
            field = new RelDataTypeFieldImpl(tableScan.getAliasName(), aliasId, newType);
        }
        Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        // create new RelNode
        if (tableScan instanceof GraphLogicalSource) {
            GraphLogicalSource source = (GraphLogicalSource) tableScan;
            SourceConfig config = new SourceConfig(source.getOpt(), labelConfig, source.getAliasName());
            RelNode newSource = graphBuilder.source(config)
                                            .build();
            ((AbstractBindableTableScan) newSource).setRowType(field);
            return result(newSource, mapping, source);

        } else if (tableScan instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) tableScan;
            RelNode input = expand.getInput(0);
            TrimResult result = trimChild(input, fieldsUsed);
            RelNode newInput = result.left;
            ExpandConfig config = new ExpandConfig(expand.getOpt(), labelConfig, field == null ? null :
                    expand.getAliasName());
            RelNode newExpand = graphBuilder.push(newInput)
                                            .expand(config)
                                            .build();
            ((AbstractBindableTableScan) newExpand).setRowType(field);
            return result(newExpand, mapping, expand);

        } else if (tableScan instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) tableScan;
            RelNode input = getV.getInput(0);
            TrimResult result = trimChild(input, fieldsUsed);
            RelNode newInput = result.left;
            GetVConfig config = new GetVConfig(getV.getOpt(), labelConfig, field == null ? null : getV.getAliasName());
            RelNode newGetV = graphBuilder.push(newInput)
                                          .getV(config)
                                          .build();
            ((AbstractBindableTableScan) newGetV).setRowType(field);
            return result(newGetV, mapping, getV);
        }

        return result(tableScan, mapping);
    }

    protected TrimResult trimChild(RelNode rel, Map<Integer, RelDataTypeField> fieldsUsed) {
        return dispatchTrimFields(rel, fieldsUsed);

    }

    final public RexGraphVariable findInput(RexGraphVariable var) {
        return var;
    }

    protected final TrimResult dispatchTrimFields(RelNode rel, Map<Integer, RelDataTypeField> fieldsUsed) {
        return graphTrimFieldsDispatcher.invoke(rel, fieldsUsed);
    }

    /***
     * Use used {@code RexGraphVariable}s, to compound a new set of fields
     * e.g:
     * used vars: [person.name, person.age, friendId, person.age]
     * return result: [person:[name,age], friendId]
     * @param vars
     * @param originalFields
     * @return newFields
     */
    protected final ImmutableSet<RelDataTypeField> compoundFields(ImmutableSet<RexGraphVariable> vars,
                                                                  List<RelDataTypeField> originalFields) {
        ImmutableSet.Builder builder = ImmutableSet.builder();
        Map<Integer, Set<@Nullable GraphProperty>> groups =
                vars.stream()
                    .collect(Collectors.groupingBy(RexGraphVariable::getAliasId, Collectors.mapping(RexGraphVariable::getProperty,
                            Collectors.toSet()))
                    );
        for (RelDataTypeField field : originalFields) {
            if (groups.containsKey(field.getIndex())) {
                if (field.getType() instanceof GraphSchemaType) {
                    GraphSchemaType original = (GraphSchemaType) field.getType();
                    // find used properties
                    Set<@Nullable GraphProperty> properties = groups.get(field.getIndex());
                    List<RelDataTypeField> fields =
                            original.getFieldList()
                                    .stream()
                                    .filter(e -> properties.contains(e.getIndex()))
                                    .collect(Collectors.toList());

                    // create new GraphSchemaType
                    RelDataType graphSchemaType = new GraphSchemaType(original.getScanOpt(),
                            original.getLabelType(), fields);
                    builder.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(), graphSchemaType));

                } else {
                    builder.add(field);
                }
            }
        }
        return builder.build();
    }

    /**
     * Concat properties of two GraphSchemaType and generate a new GraphSchemaType
     * e.g: used-> [person:[name], friend:[age]], current-> [person:[name, age], cnt]
     * after concat, the result -> [person:[name,age],friend:[age], cnt]
     *
     * @param used
     * @param current
     * @return
     */
    protected final GraphSchemaType concatGraphFields(GraphSchemaType used, GraphSchemaType current) {
        ArrayList<RelDataTypeField> fields = new ArrayList<>();
        fields.addAll(used.getFieldList());
        fields.addAll(current.getFieldList());
        List<RelDataTypeField> result = fields.stream()
                                              .distinct()
                                              .collect(Collectors.toList());
        return new GraphSchemaType(current.getScanOpt(),
                current.getLabelType(), fields);
    }


    protected ImmutableSet findUsedField(RelNode relNode) {
        ImmutableSet.Builder builder = ImmutableSet.builder();
        RelDataType rowType = relNode.getInput(0)
                                     .getRowType();
        if (relNode instanceof GraphLogicalProject) {
            GraphLogicalProject project = (GraphLogicalProject) relNode;
            for (RexNode proj : project.getProjects()) {
                proj.accept(new RexVariableAliasCollector<>(true, this::findInput))
                    .stream()
                    .forEach(builder::add);
            }
        } else if (relNode instanceof Filter) {
            Filter filter = (Filter) relNode;
            RexNode condition = filter.getCondition();
            condition.accept(new RexVariableAliasCollector<>(true, this::findInput))
                     .stream()
                     .forEach(builder::add);

        } else if (relNode instanceof GraphLogicalSort) {
            GraphLogicalSort sort = (GraphLogicalSort) relNode;
            ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
            sort.offset.accept(new RexVariableAliasCollector<>(true, this::findInput))
                       .stream()
                       .forEach(varUsedBuilder::add);

            sort.fetch.accept(new RexVariableAliasCollector<>(true, this::findInput))
                      .stream()
                      .forEach(varUsedBuilder::add);
            for (RexNode sortNode : sort.getSortExps()) {
                sortNode.accept(new RexVariableAliasCollector<>(true, this::findInput))
                        .stream()
                        .forEach(varUsedBuilder::add);
            }
        } else if (relNode instanceof GraphLogicalAggregate) {
            GraphLogicalAggregate aggregate = (GraphLogicalAggregate) relNode;
            for (RexNode var : aggregate.getGroupKey()
                                        .getVariables()) {
                var.accept(new RexVariableAliasCollector<>(true, this::findInput))
                   .stream()
                   .forEach(builder::add);
            }
            for (GraphAggCall call : aggregate.getAggCalls()) {
                for (RexNode operand : call.getOperands()) {
                    operand.accept(new RexVariableAliasCollector<>(true, this::findInput))
                           .stream()
                           .forEach(builder::add);
                }
            }
        }

        return compoundFields(builder.build(), rowType.getFieldList());
    }

}
