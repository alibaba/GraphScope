package com.alibaba.graphscope.common.ir.planner;

import static com.alibaba.graphscope.common.ir.tools.Utils.getOutputType;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.*;
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
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.mapping.IntPair;
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
        graphTrimFieldsDispatcher =
                ReflectUtil.createMethodDispatcher(
                        TrimResult.class, this, "trimFields", RelNode.class, UsedFields.class);
    }

    public RelNode trim(RelNode root) {
        UsedFields fieldsUsed = findUsedField(root);
        return dispatchTrimFields(root, fieldsUsed).left;
    }

    /**
     * @param project which to be trimmed
     * @param fieldsUsed fields used by parent
     * @return  a pair of new project relNode and mapping
     */
    public TrimResult trimFields(GraphLogicalProject project, UsedFields fieldsUsed) {
        // current project rowType
        final RelDataType rowType = project.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        final int fieldCount = rowType.getFieldCount();

        // full rowType of project
        final RelDataType fullRowType = getOutputType(project);
        List<RelDataTypeField> fullFields = fullRowType.getFieldList();
        int fullSize = fullRowType.getFieldCount();

        // rowType of input of project
        final RelDataType inputRowType = getOutputType(project.getInput(0));

        // key: id of field in project RowType, value: id of filed in input RowType
        final Mapping mapping =
                Mappings.create(MappingType.INVERSE_SURJECTION, fullSize, fieldsUsed.size());

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> aliasList = new ArrayList<>();
        UsedFields inputFieldsUsed = new UsedFields();
        int appendSize = fullSize - project.getProjects().size();

        // If project is append, we:
        // 1. Check whether the field is used by parents
        // 2. If used, create a new RexGraphVariable as project item, add it in inputFieldUsed and
        // newFieldList
        if (project.isAppend()) {
            for (int i = 0; i < appendSize; ++i) {
                RelDataTypeField field = fullFields.get(i);
                if (fieldsUsed.containsKey(field.getIndex())) {
                    RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
                    mapping.set(i, newProjects.size());
                    newProjects.add(
                            RexGraphVariable.of(
                                    field.getIndex(),
                                    i,
                                    field.getName(),
                                    parentsUsedField.getType()));
                    aliasList.add(field.getName());
                    inputFieldsUsed.add(parentsUsedField);
                }
            }
        }

        for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
            RelDataTypeField field = fieldList.get(ord.i);

            if (!fieldsUsed.containsKey(field.getIndex())) {
                continue;
            }

            RexNode proj = ord.e;
            int i = ord.i + appendSize;

            mapping.set(i, newProjects.size());

            aliasList.add(field.getName());

            // find field used by project
            List<RexGraphVariable> list =
                    ord.e.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                            .collect(Collectors.toUnmodifiableList());

            if (list.size() == 1 && field.getType() instanceof GraphSchemaType) {
                // if output type is  node/edge, we can simply think this proj just do alias
                RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
                RexGraphVariable var = list.get(0);

                // e.g `with v as person`, need to convert person.name back to v.name
                inputFieldsUsed.add(
                        new RelDataTypeFieldImpl(
                                var.getName(), var.getAliasId(), parentsUsedField.getType()));
                RexGraphVariable oldProj = (RexGraphVariable) proj;
                proj =
                        RexGraphVariable.of(
                                oldProj.getAliasId(),
                                oldProj.getIndex(),
                                oldProj.getName(),
                                parentsUsedField.getType());
            }
            newProjects.add(proj);
            varUsedBuilder.addAll(list);
        }

        // e.g: with v as person, v.age as age where age>1 and person.name <> "Li"
        // need concat inputFieldUsed(v.name) and currentFields(v.age)
        ImmutableSet<RelDataTypeField> currentFields =
                findUsedFieldsByVars(varUsedBuilder.build(), inputRowType.getFieldList());
        inputFieldsUsed.concat(currentFields);

        // trim child
        RelNode input = project.getInput();
        TrimResult trimResult = trimChild(input, inputFieldsUsed);
        RelNode newInput = trimResult.left;

        final Mapping inputMapping = trimResult.right;

        if (newProjects.isEmpty()) {
            return dummyProject(fieldCount, newInput, project);
        }

        // build new projects
        final RexVisitor<RexNode> shuttle = new RexPermuteGraphShuttle(inputMapping, newInput);

        final RelNode newProject =
                graphBuilder
                        .push(newInput)
                        .project(
                                newProjects.stream()
                                        .map(e -> e.accept(shuttle))
                                        .collect(Collectors.toList()),
                                aliasList)
                        .build();

        return result(newProject, mapping, project);
    }

    public TrimResult trimFields(GraphLogicalAggregate aggregate, UsedFields fieldsUsed) {

        List<GraphAggCall> aggCalls = new ArrayList<>();

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        UsedFields inputFieldUsed = new UsedFields(fieldsUsed);
        int keySize = aggregate.getGroupKey().groupKeyCount();

        final RelDataType rowType = aggregate.getRowType();
        final RelDataType inputRowType = getOutputType(aggregate.getInput());
        final int fieldCount = rowType.getFieldCount();
        final Mapping mapping =
                Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldsUsed.size());

        // for group by keys, do we need to collect and convert?
        // e.g:  group().by(values("v").as("a")) where v is a node?
        GraphGroupKeys keys = aggregate.getGroupKey();
        List<RexNode> groupVars = new ArrayList<>();
        for (Ord<RexNode> ord : Ord.zip(keys.getVariables())) {
            RexNode node = ord.e;
            List<RexGraphVariable> vars =
                    node.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                            .collect(Collectors.toUnmodifiableList());

            mapping.set(ord.i, ord.i);
            RelDataTypeField field = rowType.getFieldList().get(ord.i);
            // we think it's just an alias
            if (vars.size() == 1 && field.getType() instanceof GraphSchemaType) {
                RexGraphVariable var = vars.get(0);

                RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
                if (parentsUsedField != null) {
                    parentsUsedField =
                            new RelDataTypeFieldImpl(
                                    var.getName(), var.getAliasId(), parentsUsedField.getType());
                } else {
                    parentsUsedField = emptyField(field);
                }
                inputFieldUsed.add(parentsUsedField);
                groupVars.add(
                        RexGraphVariable.of(
                                var.getAliasId(),
                                var.getIndex(),
                                var.getName(),
                                parentsUsedField.getType()));
            } else {
                groupVars.add(ord.e);
                varUsedBuilder.addAll(vars);
            }
        }

        keys = new GraphGroupKeys(groupVars, keys.getAliases());

        // for aggregate calls, only record the graph variable used by calls
        for (Ord<GraphAggCall> ord : Ord.zip(aggregate.getAggCalls())) {
            GraphAggCall call = ord.e;
            RelDataTypeField field = rowType.getFieldList().get(ord.i);

            if (!fieldsUsed.containsKey(field.getIndex())) {
                continue;
            }

            for (RexNode operand : call.getOperands()) {
                operand.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                        .forEach(varUsedBuilder::add);
            }
            mapping.set(ord.i + keySize, aggCalls.size() + keySize);
            aggCalls.add(call);
        }

        // combine parents used fields and current used fields;
        ImmutableSet<RelDataTypeField> currentFields =
                findUsedFieldsByVars(varUsedBuilder.build(), inputRowType.getFieldList());
        inputFieldUsed.concat(currentFields);

        // trim child
        RelNode input = aggregate.getInput();
        TrimResult result = trimChild(input, inputFieldUsed);
        RelNode newInput = result.left;
        Mapping inputMapping = result.right;

        // create new aggregate
        final RexVisitor<RexNode> shuttle = new RexPermuteGraphShuttle(inputMapping, newInput);
        List<RexNode> vars =
                keys.getVariables().stream()
                        .map(var -> var.accept(shuttle))
                        .collect(Collectors.toList());
        GraphGroupKeys newKeys = new GraphGroupKeys(vars, keys.getAliases());

        List<RelBuilder.AggCall> newAggCalls =
                aggCalls.stream()
                        .map(
                                call -> {
                                    List<RexNode> operands =
                                            call.getOperands().stream()
                                                    .map(operand -> operand.accept(shuttle))
                                                    .collect(Collectors.toList());
                                    GraphAggCall newCall =
                                            new GraphAggCall(
                                                    call.getCluster(),
                                                    call.getAggFunction(),
                                                    operands);
                                    newCall.as(call.getAlias());
                                    return newCall;
                                })
                        .collect(Collectors.toList());

        RelNode newAggregate = graphBuilder.push(newInput).aggregate(newKeys, newAggCalls).build();
        return result(newAggregate, mapping, aggregate);
    }

    public TrimResult trimFields(GraphLogicalSort sort, UsedFields fieldsUsed) {
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        RelNode input = sort.getInput();
        final RelDataType inputRowType = getOutputType(input);
        UsedFields inputFieldsUsed = new UsedFields(fieldsUsed);

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();

        if (offset != null) {
            offset.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                    .forEach(varUsedBuilder::add);
        }

        if (fetch != null) {
            fetch.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                    .forEach(varUsedBuilder::add);
        }

        for (RexNode expr : sort.getSortExps()) {
            expr.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                    .forEach(varUsedBuilder::add);
        }

        ImmutableSet<RelDataTypeField> current =
                findUsedFieldsByVars(varUsedBuilder.build(), inputRowType.getFieldList());

        inputFieldsUsed.concat(current);

        // trim children
        TrimResult trimResult = trimChild(input, inputFieldsUsed);
        RelNode newInput = trimResult.left;
        Mapping inputMapping = trimResult.right;

        // build new sort
        final RexVisitor<RexNode> shuttle = new RexPermuteGraphShuttle(inputMapping, newInput);
        RexNode newOffset = offset == null ? null : offset.accept(shuttle);
        RexNode newFetch = fetch == null ? null : fetch.accept(shuttle);
        List<RexNode> newSortExprs =
                sort.getSortExps().stream()
                        .map(e -> e.accept(shuttle))
                        .collect(Collectors.toUnmodifiableList());

        RelNode newSort =
                graphBuilder.push(newInput).sortLimit(newOffset, newFetch, newSortExprs).build();
        return result(newSort, inputMapping, sort);
    }

    public TrimResult trimFields(LogicalFilter filter, UsedFields fieldsUsed) {
        RelDataType inputRowType = getOutputType(filter.getInput());
        UsedFields inputFieldsUsed = new UsedFields(fieldsUsed);

        // Find columns and PropertyRef used by filter.
        RexNode condition = filter.getCondition();

        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
        condition.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                .forEach(varUsedBuilder::add);
        ImmutableSet<RelDataTypeField> current =
                findUsedFieldsByVars(varUsedBuilder.build(), inputRowType.getFieldList());
        inputFieldsUsed.concat(current);

        // Trim child
        RelNode input = filter.getInput();
        TrimResult trimResult = trimChild(input, inputFieldsUsed);
        RelNode newInput = trimResult.left;
        Mapping inputMapping = trimResult.right;

        if (Objects.equals(input, newInput)) {
            return result(filter, inputMapping);
        }

        // use inputMapping shuttle conditions
        final RexVisitor<RexNode> shuttle = new RexPermuteGraphShuttle(inputMapping, newInput);
        RexNode newCondition = condition.accept(shuttle);

        RelNode newFilter =
                graphBuilder.push(newInput).filter(filter.getVariablesSet(), newCondition).build();
        return result(newFilter, inputMapping, filter);
    }

    public TrimResult trimFields(GraphLogicalSingleMatch singleMatch, UsedFields fieldsUsed) {
        RelNode sentence = singleMatch.getSentence();
        int fieldCount = singleMatch.getRowType().getFieldCount();
        TrimResult result = trimChild(sentence, fieldsUsed);
        RelNode newInput = result.left;

        final Mapping mapping =
                Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        if (Objects.equals(sentence, result)) {
            return result(singleMatch, mapping);
        }

        RelNode newMatch = graphBuilder.match(newInput, singleMatch.getMatchOpt()).build();
        return result(newMatch, mapping, singleMatch);
    }

    public TrimResult trimFields(GraphLogicalMultiMatch multiMatch, UsedFields fieldsUsed) {
        List<RelNode> sentences = multiMatch.getSentences();
        List<RelNode> newInputs = Collections.emptyList();
        int fieldCount = multiMatch.getRowType().getFieldCount();
        boolean changed = false;
        for (RelNode node : sentences) {
            TrimResult result = trimChild(node, fieldsUsed);
            newInputs.add(result.left);
            if (!changed && Objects.equals(result.left, node)) {
                changed = true;
            }
        }
        Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        RelNode newMatch =
                graphBuilder
                        .match(newInputs.get(0), newInputs.subList(1, sentences.size()))
                        .build();
        return result(newMatch, mapping, multiMatch);
    }

    public TrimResult trimFields(GraphLogicalPathExpand pathExpand, UsedFields fieldsUsed) {
        RelNode input = pathExpand.getInput();
        RelNode expand = pathExpand.getExpand();
        RelNode getV = pathExpand.getGetV();

        int fieldCount = pathExpand.getRowType().getFieldCount();
        final Mapping mapping =
                Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        // trim children
        RelNode newInput = trimChild(input, fieldsUsed).left;
        RelNode newExpand = trimChild(expand, fieldsUsed).left;
        RelNode newGetV = trimChild(getV, fieldsUsed).left;
        GraphLogicalPathExpand newPathExpand =
                GraphLogicalPathExpand.create(
                        (GraphOptCluster) pathExpand.getCluster(),
                        List.of(),
                        newInput,
                        newExpand,
                        newGetV,
                        pathExpand.getOffset(),
                        pathExpand.getFetch(),
                        pathExpand.getResultOpt(),
                        pathExpand.getPathOpt(),
                        pathExpand.getUntilCondition(),
                        pathExpand.getAliasName(),
                        pathExpand.getStartAlias());
        return result(newPathExpand, mapping, pathExpand);
    }

    public TrimResult trimFields(Join join, UsedFields fieldsUsed) {
        UsedFields inputFieldsUsed = new UsedFields(fieldsUsed);
        ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();

        List<RelDataTypeField> inputFields =
                new ArrayList<>(join.getLeft().getRowType().getFieldList());
        inputFields.addAll(join.getRight().getRowType().getFieldList());
        final int inputFieldCount = inputFields.size();
        int newInputFieldCount = 0;

        // Find used properties in  join conditions
        final RexNode condition = join.getCondition();
        condition.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
                .forEach(varUsedBuilder::add);
        ImmutableSet<RelDataTypeField> current =
                findUsedFieldsByVars(varUsedBuilder.build(), inputFields);
        inputFieldsUsed.concat(current);

        final List<RelNode> newInputs = new ArrayList<>(2);
        final List<Mapping> inputMappings = new ArrayList<>();

        for (RelNode input : join.getInputs()) {
            TrimResult result = trimChild(input, inputFieldsUsed);
            newInputs.add(result.left);
            inputMappings.add(result.right);
            newInputFieldCount += result.right.getTargetCount();
        }

        Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION, inputFieldCount, newInputFieldCount);

        for (int i = 0; i < inputMappings.size(); i++) {
            Mapping inputMapping = inputMappings.get(i);
            for (IntPair pair : inputMapping) {
                mapping.set(pair.source, pair.target);
            }
        }

        // Build new join.
        final RexVisitor<RexNode> shuttle =
                new RexPermuteGraphShuttle(mapping, newInputs.get(0), newInputs.get(1));
        RexNode newConditionExpr = condition.accept(shuttle);

        graphBuilder.push(newInputs.get(0));
        graphBuilder.push(newInputs.get(1));

        // For SemiJoins and AntiJoins only map fields from the left-side
        if (join.getJoinType() == JoinRelType.SEMI || join.getJoinType() == JoinRelType.ANTI) {
            Mapping inputMapping = inputMappings.get(0);
            mapping =
                    Mappings.create(
                            MappingType.INVERSE_SURJECTION,
                            join.getRowType().getFieldCount(),
                            inputMapping.getTargetCount());
            for (IntPair pair : inputMapping) {
                mapping.set(pair.source, pair.target);
            }
        }

        graphBuilder.join(join.getJoinType(), newConditionExpr);
        return result(graphBuilder.build(), mapping, join);
    }

    public TrimResult trimFields(AbstractBindableTableScan tableScan, UsedFields fieldsUsed) {
        RelDataType rowType = tableScan.getRowType();
        int aliasId = tableScan.getAliasId();
        int fieldCount = rowType.getFieldCount();
        LabelConfig labelConfig = new LabelConfig(false);
        tableScan.getTableConfig().getTables().stream()
                .map(e -> e.getQualifiedName().get(0))
                .forEach(labelConfig::addLabel);

        // create new RowType of table scan
        RelDataTypeField field;
        boolean setAlias = false;
        if (fieldsUsed.containsKey(aliasId)) {
            field = fieldsUsed.get(aliasId);
            setAlias = true;
        } else {
            RelDataTypeField origin = rowType.getFieldList().get(0);
            field = emptyField(origin);
        }

        Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            mapping.set(i, i);
        }

        // create new RelNode
        if (tableScan instanceof GraphLogicalSource) {
            GraphLogicalSource source = (GraphLogicalSource) tableScan;
            SourceConfig config =
                    new SourceConfig(source.getOpt(), labelConfig, source.getAliasName());
            RelNode newSource = graphBuilder.source(config).build();
            ((AbstractBindableTableScan) newSource)
                    .setSchemaType((GraphSchemaType) field.getType());
            return result(newSource, mapping, source);

        } else if (tableScan instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) tableScan;
            RelNode input = expand.getInput(0);
            TrimResult result = trimChild(input, fieldsUsed);
            RelNode newInput = result.left;
            ExpandConfig config =
                    new ExpandConfig(expand.getOpt(), labelConfig, expand.getAliasName());
            RelNode newExpand = graphBuilder.push(newInput).expand(config).build();
            ((AbstractBindableTableScan) newExpand)
                    .setSchemaType((GraphSchemaType) field.getType());
            return result(newExpand, mapping, expand);

        } else if (tableScan instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) tableScan;
            RelNode input = getV.getInput(0);
            TrimResult result = trimChild(input, fieldsUsed);
            RelNode newInput = result.left;
            GetVConfig config = new GetVConfig(getV.getOpt(), labelConfig, getV.getAliasName());
            RelNode newGetV = graphBuilder.push(newInput).getV(config).build();
            ((AbstractBindableTableScan) newGetV).setSchemaType((GraphSchemaType) field.getType());
            return result(newGetV, mapping, getV);
        }

        return result(tableScan, mapping);
    }

    protected TrimResult trimChild(RelNode rel, UsedFields fieldsUsed) {
        return dispatchTrimFields(rel, fieldsUsed);
    }

    public final RexGraphVariable findInput(RexGraphVariable var) {
        return var;
    }

    protected final TrimResult dispatchTrimFields(RelNode rel, UsedFields fieldsUsed) {
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
    protected final ImmutableSet<RelDataTypeField> findUsedFieldsByVars(
            ImmutableSet<RexGraphVariable> vars, List<RelDataTypeField> originalFields) {
        ImmutableSet.Builder builder = ImmutableSet.builder();
        Map<Integer, Set<@Nullable GraphProperty>> groups =
                vars.stream()
                        .collect(
                                Collectors.groupingBy(
                                        RexGraphVariable::getAliasId,
                                        Collectors.mapping(
                                                RexGraphVariable::getProperty,
                                                Collectors.toSet())));
        for (RelDataTypeField field : originalFields) {
            if (groups.containsKey(field.getIndex())) {
                if (field.getType() instanceof GraphSchemaType) {
                    GraphSchemaType original = (GraphSchemaType) field.getType();
                    // find used properties
                    Set<@Nullable GraphProperty> properties = groups.get(field.getIndex());
                    List<RelDataTypeField> fields =
                            original.getFieldList().stream()
                                    .filter(e -> isUsedProperty(properties, e))
                                    .collect(Collectors.toList());

                    // create new GraphSchemaType
                    RelDataType graphSchemaType =
                            new GraphSchemaType(
                                    original.getScanOpt(), original.getLabelType(), fields);
                    builder.add(
                            new RelDataTypeFieldImpl(
                                    field.getName(), field.getIndex(), graphSchemaType));

                } else {
                    builder.add(field);
                }
            }
        }
        return builder.build();
    }

    /**
     * @param properties
     * @param field
     * @return whether the properties are used
     */
    private boolean isUsedProperty(
            Set<@Nullable GraphProperty> properties, RelDataTypeField field) {
        for (GraphProperty property : properties) {
            if (property != null) {
                if (property.getOpt() == GraphProperty.Opt.ALL) {
                    return true;
                }
                boolean isEqual =
                        property.getKey().getOpt() == GraphNameOrId.Opt.NAME
                                ? Objects.equals(property.getKey().getName(), field.getName())
                                : property.getKey().getId() == field.getIndex();
                if (isEqual) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * find usedFields of the root of the RelPlan tree with empty properties
     *
     * @param root
     * @return
     */
    protected UsedFields findUsedField(RelNode root) {
        RelDataType rowType = root.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();
        Set<RelDataTypeField> set =
                fields.stream().map(field -> emptyField(field)).collect(Collectors.toSet());
        return new UsedFields(set);
    }

    /**
     * Empty properties if the type of field is {@Code GraphSchemaType}.
     *
     * @param field
     * @return field after empty
     */
    private RelDataTypeField emptyField(RelDataTypeField field) {
        if (field.getType() instanceof GraphSchemaType) {
            GraphSchemaType original = (GraphSchemaType) field.getType();
            GraphSchemaType newType =
                    new GraphSchemaType(
                            original.getScanOpt(), original.getLabelType(), new ArrayList<>());
            return new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType);
        } else {
            return field;
        }
    }

    public class UsedFields {
        private final Map<Integer, RelDataTypeField> fieldMap;

        public UsedFields() {
            fieldMap = new HashMap<>();
        }

        public UsedFields(Set<RelDataTypeField> fields) {
            fieldMap = new HashMap<>();
            for (RelDataTypeField field : fields) {
                fieldMap.put(field.getIndex(), field);
            }
        }

        public UsedFields(UsedFields fields) {
            fieldMap = new HashMap<>(fields.fieldMap);
        }

        /**
         * Add single field if it doesn't contain such field, just add it. Otherwise: if the type of the
         * field is NOT {@code GraphSchemaType}, do nothing if the type of the field is {@code
         * GraphSchemaType}, combine current field and the param
         *
         * @param field
         */
        public void add(RelDataTypeField field) {
            if (fieldMap.containsKey(field.getIndex())) {
                if (field.getType() instanceof GraphSchemaType) {
                    GraphSchemaType lhs =
                            (GraphSchemaType) fieldMap.get(field.getIndex()).getType();
                    GraphSchemaType rhs = (GraphSchemaType) field.getType();
                    List<RelDataTypeField> newFields = new ArrayList<>();
                    newFields.addAll(lhs.getFieldList());
                    newFields.addAll(rhs.getFieldList());
                    newFields = newFields.stream().distinct().collect(Collectors.toList());
                    GraphSchemaType newType =
                            new GraphSchemaType(rhs.getScanOpt(), rhs.getLabelType(), newFields);
                    fieldMap.put(
                            field.getIndex(),
                            new RelDataTypeFieldImpl(field.getName(), field.getIndex(), newType));
                }
            } else {
                fieldMap.put(field.getIndex(), field);
            }
        }

        /**
         * Concat properties of two RowType and generate a new RowType e.g: current-> [person:[name],
         * friend:[age]], fields-> [person:[name, age], cnt] after concat, the result ->
         * [person:[name,age],friend:[age], cnt]
         *
         * @param fields
         */
        public void concat(Iterable<RelDataTypeField> fields) {
            for (RelDataTypeField field : fields) {
                add(field);
            }
        }

        /**
         * Get {@code RelDataTypeField} by field index
         *
         * @param i index of field
         * @return field, return null if no such field
         */
        public final @Nullable RelDataTypeField get(int i) {
            return fieldMap.getOrDefault(i, null);
        }

        public final boolean containsKey(int i) {
            return fieldMap.containsKey(i);
        }

        public final int size() {
            return fieldMap.size();
        }
    }
}
