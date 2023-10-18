package com.alibaba.graphscope.common.ir.planner;

import static com.alibaba.graphscope.common.ir.tools.Utils.getOutputType;

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
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

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
   * @param project
   * @param fieldsUsed
   * @return
   */
  public TrimResult trimFields(GraphLogicalProject project, UsedFields fieldsUsed) {
    final RelDataType rowType = project.getRowType();
    final RelDataType inputRowType = getOutputType(project.getInput());
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    final int fieldCount = rowType.getFieldCount();
    RelDataType fullRowType = project.isAppend() ? getOutputType(project) : rowType;

    // key: id of field in project RowType, value: id of filed in input RowType
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION, fullRowType.getFieldCount(), fieldsUsed.size());

    ImmutableSet.Builder varUsedBuilder = ImmutableSet.builder();
    List<RexNode> newProjects = new ArrayList<>();
    List<String> aliasList = new ArrayList<>();
    UsedFields inputFieldsUsed = new UsedFields();

    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      RelDataTypeField field = fieldList.get(ord.i);

      if (!fieldsUsed.containsKey(field.getIndex())) {
        continue;
      }

      RexNode proj = ord.e;

      mapping.set(ord.i, newProjects.size());
      newProjects.add(proj);
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
            new RelDataTypeFieldImpl(var.getName(), var.getAliasId(), parentsUsedField.getType()));
      }
      varUsedBuilder.addAll(list);
    }

    // If project is append, we:
    // 1. Check whether the field is used by parents
    // 2. If used, create a new RexGraphVariable as project item, add it in inputFieldUsed and
    // newFieldList
    if (project.isAppend()) {
      List<RelDataTypeField> fullFields = fullRowType.getFieldList();
      int fullSize = fullFields.size();
      for (int i = fieldList.size(); i < fullSize; ++i) {
        RelDataTypeField field = fullFields.get(i);
        if (fieldsUsed.containsKey(field.getIndex())) {
          RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
          mapping.set(i, newProjects.size());
          newProjects.add(
              RexGraphVariable.of(field.getIndex(), i, field.getName(), field.getType()));
          aliasList.add(field.getName());
          inputFieldsUsed.add(parentsUsedField);
        }
      }
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

    if (newProjects.size() == 0) {
      return dummyProject(fieldCount, newInput, project);
    }

    // build new projects
    final RexVisitor<RexNode> shuttle = new RexPermuteGraphShuttle(inputMapping, newInput);
    // TODO(huaiyu): change graphSchema

    final RelNode newProject =
        graphBuilder
            .push(newInput)
            .project(
                newProjects.stream().map(e -> e.accept(shuttle)).collect(Collectors.toList()),
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

        if (fieldsUsed.containsKey(field.getIndex())) {
          RelDataTypeField parentsUsedField = fieldsUsed.get(field.getIndex());
          inputFieldUsed.add(
              new RelDataTypeFieldImpl(
                  var.getName(), var.getAliasId(), parentsUsedField.getType()));
        } else {
          inputFieldUsed.add(emptyField(field));
        }
      }
      varUsedBuilder.addAll(vars);
    }

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
        keys.getVariables().stream().map(var -> var.accept(shuttle)).collect(Collectors.toList());
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
                      new GraphAggCall(call.getCluster(), call.getAggFunction(), operands);
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

    if(offset!=null){
      offset.accept(new RexVariableAliasCollector<>(true, this::findInput)).stream()
            .forEach(varUsedBuilder::add);
    }

    if(fetch!=null){
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
    RexNode newOffset =offset==null? null:offset.accept(shuttle);
    RexNode newFetch = fetch==null?null:fetch.accept(shuttle);
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

    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldCount);
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
        graphBuilder.match(newInputs.get(0), newInputs.subList(1, sentences.size())).build();
    return result(newMatch, mapping, multiMatch);
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
    if (fieldsUsed.containsKey(aliasId)) {
      field = fieldsUsed.get(aliasId);
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
      SourceConfig config = new SourceConfig(source.getOpt(), labelConfig, source.getAliasName());
      RelNode newSource = graphBuilder.source(config).build();
      ((AbstractBindableTableScan) newSource).setRowType(field);
      return result(newSource, mapping, source);

    } else if (tableScan instanceof GraphLogicalExpand) {
      GraphLogicalExpand expand = (GraphLogicalExpand) tableScan;
      RelNode input = expand.getInput(0);
      TrimResult result = trimChild(input, fieldsUsed);
      RelNode newInput = result.left;
      ExpandConfig config =
          new ExpandConfig(
              expand.getOpt(), labelConfig, field == null ? null : expand.getAliasName());
      RelNode newExpand = graphBuilder.push(newInput).expand(config).build();
      ((AbstractBindableTableScan) newExpand).setRowType(field);
      return result(newExpand, mapping, expand);

    } else if (tableScan instanceof GraphLogicalGetV) {
      GraphLogicalGetV getV = (GraphLogicalGetV) tableScan;
      RelNode input = getV.getInput(0);
      TrimResult result = trimChild(input, fieldsUsed);
      RelNode newInput = result.left;
      GetVConfig config =
          new GetVConfig(getV.getOpt(), labelConfig, field == null ? null : getV.getAliasName());
      RelNode newGetV = graphBuilder.push(newInput).getV(config).build();
      ((AbstractBindableTableScan) newGetV).setRowType(field);
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
                    Collectors.mapping(RexGraphVariable::getProperty, Collectors.toSet())));
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
              new GraphSchemaType(original.getScanOpt(), original.getLabelType(), fields);
          builder.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(), graphSchemaType));

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
  private boolean isUsedProperty(Set<@Nullable GraphProperty> properties, RelDataTypeField field) {
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
          new GraphSchemaType(original.getScanOpt(), original.getLabelType(), new ArrayList<>());
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
          GraphSchemaType lhs = (GraphSchemaType) fieldMap.get(field.getIndex()).getType();
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
