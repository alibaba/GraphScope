/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/tools/RelBuilder.java
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.calcite.tools;

import static com.alibaba.graphscope.common.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.calcite.rel.*;
import com.alibaba.graphscope.common.calcite.rel.graph.*;
import com.alibaba.graphscope.common.calcite.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.calcite.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.calcite.rel.type.TableConfig;
import com.alibaba.graphscope.common.calcite.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.calcite.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.calcite.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.calcite.rel.type.order.GraphRelCollations;
import com.alibaba.graphscope.common.calcite.rex.RexCallBinding;
import com.alibaba.graphscope.common.calcite.rex.RexGraphVariable;
import com.alibaba.graphscope.common.calcite.rex.RexVariableAliasChecker;
import com.alibaba.graphscope.common.calcite.tools.config.*;
import com.alibaba.graphscope.common.calcite.type.GraphSchemaType;
import com.alibaba.graphscope.common.calcite.util.Static;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

/**
 * Integrate interfaces to build algebra structures,
 * including {@link RexNode} for expressions and {@link RelNode} for operators
 */
public class GraphBuilder extends RelBuilder {
    /**
     * @param context      not used currently
     * @param cluster      get {@link org.apache.calcite.rex.RexBuilder} (to build {@code RexNode})
     *                     and other global resources (not used currently) from it
     * @param relOptSchema get graph schema from it
     */
    protected GraphBuilder(
            @Nullable Context context, GraphOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    /**
     * @param context
     * @param cluster
     * @param relOptSchema
     * @return
     */
    public static GraphBuilder create(
            @Nullable Context context, GraphOptCluster cluster, RelOptSchema relOptSchema) {
        return new GraphBuilder(context, cluster, relOptSchema);
    }

    /**
     * validate and build an algebra structure of {@link GraphLogicalSource}.
     *
     * how to validate:
     * 1. validate the existence of the given labels in config,
     * if exist then derive the {@link GraphSchemaType} of the given labels and keep the type in {@link RelNode#getRowType()},
     * otherwise throw exceptions
     *
     * 2. validate the existence of the given alias in config, if exist throw exceptions
     *
     * @param config
     * @return
     */
    public GraphBuilder source(SourceConfig config) {
        String aliasName = AliasInference.inferDefault(config.getAlias(), new HashSet<>());
        int aliasId = generateAliasId(aliasName, null);
        RelNode source =
                GraphLogicalSource.create(
                        (GraphOptCluster) cluster,
                        getHints(config.getOpt().name(), aliasName, aliasId),
                        getTableConfig(config.getLabels()));
        push(source);
        return this;
    }

    /**
     * validate and build an algebra structure of {@link GraphLogicalExpand}
     *
     * @param config
     * @return
     */
    public GraphBuilder expand(ExpandConfig config) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        String aliasName =
                AliasInference.inferDefault(config.getAlias(), uniqueNameList(input, false, true));
        int aliasId = generateAliasId(aliasName, input);
        RelNode expand =
                GraphLogicalExpand.create(
                        (GraphOptCluster) cluster,
                        getHints(config.getOpt().name(), aliasName, aliasId),
                        input,
                        getTableConfig(config.getLabels()));
        replaceTop(expand);
        return this;
    }

    /**
     * validate and build an algebra structure of {@link GraphLogicalGetV}
     *
     * @param config
     * @return
     */
    public GraphBuilder getV(GetVConfig config) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        String aliasName =
                AliasInference.inferDefault(config.getAlias(), uniqueNameList(input, false, true));
        int aliasId = generateAliasId(aliasName, input);
        RelNode getV =
                GraphLogicalGetV.create(
                        (GraphOptCluster) cluster,
                        getHints(config.getOpt().name(), aliasName, aliasId),
                        input,
                        getTableConfig(config.getLabels()));
        replaceTop(getV);
        return this;
    }

    /**
     * build an algebra structure of {@link GraphLogicalPathExpand}
     *
     * @param config
     * @return
     */
    public GraphBuilder pathExpand(PathExpandConfig config) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        String aliasName =
                AliasInference.inferDefault(config.getAlias(), uniqueNameList(input, false, true));
        RexNode offsetNode = config.getOffset() <= 0 ? null : literal(config.getOffset());
        RexNode fetchNode = config.getFetch() < 0 ? null : literal(config.getFetch());
        RelNode expand = Objects.requireNonNull(config.getExpand());
        RelNode getV = Objects.requireNonNull(config.getGetV());
        RelNode pathExpand =
                GraphLogicalPathExpand.create(
                        (GraphOptCluster) cluster,
                        getHints(
                                config.getPathOpt().name(),
                                config.getResultOpt().name(),
                                aliasName,
                                generateAliasId(aliasName, input)),
                        input,
                        expand,
                        getV,
                        offsetNode,
                        fetchNode);
        replaceTop(pathExpand);
        return this;
    }

    /**
     * convert user-given config to {@code TableConfig},
     * derive all table types (labels with properties) depending on the user given labels
     * @param labelConfig
     * @return
     */
    public TableConfig getTableConfig(LabelConfig labelConfig) {
        if (!labelConfig.isAll()) {
            ObjectUtils.requireNonEmpty(labelConfig.getLabels());
            List<RelOptTable> tables = new ArrayList<>();
            for (String label : labelConfig.getLabels()) {
                tables.add(relOptSchema.getTableForMember(ImmutableList.of(label)));
            }
            return new TableConfig(tables).isAll(labelConfig.isAll());
        } else {
            throw new UnsupportedOperationException(
                    "Non specific labels in table scan not supported currently");
        }
    }

    public List<RelHint> getHints(String optName, String aliasName, int aliasId) {
        RelHint optHint = RelHint.builder("opt").hintOption(optName).build();
        RelHint aliasHint =
                RelHint.builder("alias")
                        .hintOption("name", Objects.requireNonNull(aliasName))
                        .hintOption("id", String.valueOf(aliasId))
                        .build();
        return ImmutableList.of(optHint, aliasHint);
    }

    private List<RelHint> getHints(
            String pathOptName, String resultOptName, String aliasName, int aliasId) {
        RelHint optHint =
                RelHint.builder("opt")
                        .hintOption("path", pathOptName)
                        .hintOption("result", resultOptName)
                        .build();
        RelHint aliasHint =
                RelHint.builder("alias")
                        .hintOption("name", aliasName)
                        .hintOption("id", String.valueOf(aliasId))
                        .build();
        return ImmutableList.of(optHint, aliasHint);
    }

    /**
     * get all aliases stored by previous operators, to avoid duplicate alias creation
     * @param input the input operator
     * @param containsAll if the input operator contains all stored alias
     * @param isAppend if the current operator need to keep the history
     * @return
     */
    private Set<String> uniqueNameList(
            @Nullable RelNode input, boolean containsAll, boolean isAppend) {
        Set<String> uniqueNames = new HashSet<>();
        if (!isAppend || input == null) return uniqueNames;
        for (RelDataTypeField field : input.getRowType().getFieldList()) {
            uniqueNames.add(field.getName());
        }
        if (!containsAll && ObjectUtils.isNotEmpty(input.getInputs())) {
            uniqueNames.addAll(uniqueNameList(input.getInput(0), containsAll, isAppend));
        }
        return uniqueNames;
    }

    /**
     * generate a new alias id for the given alias name
     *
     * @param alias
     * @param input
     * @return
     */
    private int generateAliasId(@Nullable String alias, @Nullable RelNode input) {
        RelOptCluster cluster = getCluster();
        return ((GraphOptCluster) cluster).getIdGenerator().generate(alias, input);
    }

    /**
     * validate and build an algebra structure of {@link GraphLogicalSingleMatch}
     * which wrappers all graph operators in one sentence.
     *
     * how to validate:
     * check the graph pattern (lookup from the graph schema and check whether the links are all valid)
     * denoted by each sentence one by one.
     *
     * @param single single sentence
     * @param opt anti or optional
     */
    public GraphBuilder match(RelNode single, MatchOpt opt) {
        RelNode input = size() > 0 ? peek() : null;
        RelNode match =
                GraphLogicalSingleMatch.create((GraphOptCluster) cluster, null, input, single, opt);
        if (size() > 0) pop();
        push(match);
        return this;
    }

    /**
     * validate and build an algebra structure of {@link GraphLogicalMultiMatch}
     * which wrappers all graph operators in multiple sentences (multiple sentences are inner join).
     *
     * how to validate:
     * check the graph pattern (lookup from the graph schema and check whether the links are all valid)
     * denoted by each sentence one by one.
     *
     * @return
     */
    public GraphBuilder match(RelNode first, Iterable<? extends RelNode> others) {
        RelNode input = size() > 0 ? peek() : null;
        RelNode match =
                GraphLogicalMultiMatch.create(
                        (GraphOptCluster) cluster,
                        null,
                        input,
                        first,
                        ImmutableList.copyOf(others));
        if (size() > 0) pop();
        push(match);
        return this;
    }

    /**
     * validate and build {@link RexGraphVariable} from a given alias (i.e. "a")
     *
     * @param alias
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias) {
        alias = (alias == null) ? AliasInference.DEFAULT_NAME : alias;
        RelDataTypeField aliasField = getAliasField(alias);
        return RexGraphVariable.of(
                aliasField.getIndex(), AliasInference.SIMPLE_NAME(alias), aliasField.getType());
    }

    /**
     * validate and build {@link RexGraphVariable} from a given variable containing fieldName (i.e. "a.name" or "name")
     *
     * @param alias
     * @param property
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias, String property) {
        alias = (alias == null) ? AliasInference.DEFAULT_NAME : alias;
        Objects.requireNonNull(property);
        RelDataTypeField aliasField = getAliasField(alias);
        if (!(aliasField.getType() instanceof GraphSchemaType)) {
            throw RESOURCE.incompatibleTypes(
                            "Graph Element", GraphSchemaType.class, aliasField.getType().getClass())
                    .ex();
        }
        GraphSchemaType graphType = (GraphSchemaType) aliasField.getType();
        List<String> properties = new ArrayList<>();
        for (RelDataTypeField pField : graphType.getFieldList()) {
            if (pField.getName().equals(property)) {
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        pField.getIndex(),
                        AliasInference.SIMPLE_NAME(alias) + Static.DELIMITER + property,
                        pField.getType());
            }
            properties.add(pField.getName());
        }
        throw new IllegalArgumentException(
                "{property="
                        + property
                        + "} "
                        + "not found; expected properties are: "
                        + properties);
    }

    /**
     * get {@code RelDataTypeField} by the given alias, for {@code RexGraphVariable} inference
     * @param alias
     * @return
     */
    private RelDataTypeField getAliasField(String alias) {
        Objects.requireNonNull(alias);
        List<String> aliases = new ArrayList<>();
        if (size() > 0) {
            RelNode cur = peek();
            RelRecordType rowType = (RelRecordType) cur.getRowType();
            if (alias == AliasInference.DEFAULT_NAME && rowType.getFieldList().size() == 1) {
                return rowType.getFieldList().get(0);
            }
            for (RelDataTypeField field : rowType.getFieldList()) {
                if (field.getName().equals(alias)) {
                    return field;
                }
                aliases.add(AliasInference.SIMPLE_NAME(field.getName()));
            }
        }
        throw new IllegalArgumentException(
                "{alias="
                        + AliasInference.SIMPLE_NAME(alias)
                        + "} "
                        + "not found; expected aliases are: "
                        + aliases);
    }

    /**
     * build complex expressions denoted by {@link org.apache.calcite.rex.RexCall} from the given parameters
     *
     * @param operator provides type checker and inference
     * @param operands
     * @return
     */
    @Override
    public RexNode call(SqlOperator operator, RexNode... operands) {
        List<RexNode> operandList = ImmutableList.copyOf(operands);
        if (!isCurrentSupported(operator)) {
            throw new UnsupportedOperationException(
                    "operator " + operator.getKind().name() + " not supported");
        }
        RexCallBinding callBinding =
                new RexCallBinding(getTypeFactory(), operator, operandList, ImmutableList.of());
        // check count of operands, if fail throw exceptions
        operator.validRexOperands(callBinding.getOperandCount(), Litmus.THROW);
        // check type of each operand, if fail throw exceptions
        operator.checkOperandTypes(callBinding, true);
        // derive type
        RelDataType type = operator.inferReturnType(callBinding);
        final RexBuilder builder = cluster.getRexBuilder();
        return builder.makeCall(type, operator, operandList);
    }

    private boolean isCurrentSupported(SqlOperator operator) {
        SqlKind sqlKind = operator.getKind();
        return sqlKind.belongsTo(SqlKind.BINARY_ARITHMETIC)
                || sqlKind.belongsTo(Static.BINARY_COMPARISON)
                || sqlKind == SqlKind.AND
                || sqlKind == SqlKind.OR
                || sqlKind == SqlKind.DESCENDING;
    }

    @Override
    public GraphBuilder filter(RexNode... conditions) {
        return filter(ImmutableList.copyOf(conditions));
    }

    @Override
    public GraphBuilder filter(Iterable<? extends RexNode> conditions) {
        ObjectUtils.requireNonEmpty(conditions);
        // make sure all conditions have the Boolean return type
        for (RexNode condition : conditions) {
            RelDataType type = condition.getType();
            if (!(type instanceof BasicSqlType) || type.getSqlTypeName() != SqlTypeName.BOOLEAN) {
                throw new IllegalArgumentException(
                        "filter condition "
                                + condition
                                + " should return Boolean value, but is "
                                + type);
            }
        }
        GraphBuilder builder = (GraphBuilder) super.filter(ImmutableSet.of(), conditions);
        // fuse filter with the previous table scan if meets the conditions
        Filter filter;
        AbstractBindableTableScan tableScan;
        if ((filter = topFilter()) != null && (tableScan = inputTableScan(filter)) != null) {
            RexNode condition = filter.getCondition();
            RexVariableAliasChecker checker =
                    new RexVariableAliasChecker(
                            true,
                            ImmutableList.of(tableScan.getAliasId(), AliasInference.DEFAULT_ID));
            condition.accept(checker);
            // fuze all conditions into table scan
            if (checker.isAll()) {
                // pop the filter from the inner stack
                builder.pop();
                // add the condition in table scan
                tableScan.setFilters(ImmutableList.of(condition));
                push(tableScan);
            }
        }
        return builder;
    }

    // return the top node if its type is Filter, otherwise null
    private Filter topFilter() {
        if (this.size() > 0 && this.peek() instanceof Filter) {
            return (Filter) this.peek();
        } else {
            return null;
        }
    }

    // return the input node of the Filter if its type is TableScan, otherwise null
    private AbstractBindableTableScan inputTableScan(RelNode filter) {
        Objects.requireNonNull(filter);
        List<RelNode> inputs = filter.getInputs();
        if (inputs != null
                && inputs.size() == 1
                && inputs.get(0) instanceof AbstractBindableTableScan) {
            return (AbstractBindableTableScan) inputs.get(0);
        } else {
            return null;
        }
    }

    @Override
    public GraphBuilder project(Iterable<? extends RexNode> nodes) {
        return project(nodes, ImmutableList.of(), false);
    }

    @Override
    public GraphBuilder project(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> aliases,
            boolean isAppend) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        Config config = ClassUtils.getFieldValue(RelBuilder.class, this, "config");
        RexSimplify simplifier = ClassUtils.getFieldValue(RelBuilder.class, this, "simplifier");

        List<RexNode> nodeList = Lists.newArrayList(nodes);
        List<@Nullable String> fieldNameList = Lists.newArrayList(aliases);

        // Simplify expressions.
        if (config.simplify()) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.set(i, simplifier.simplifyPreservingType(nodeList.get(i)));
            }
        }
        fieldNameList =
                AliasInference.inferProject(
                        nodeList, fieldNameList, uniqueNameList(input, true, isAppend));
        RelNode project =
                GraphLogicalProject.create(
                        (GraphOptCluster) getCluster(),
                        ImmutableList.of(),
                        input,
                        nodeList,
                        deriveType(nodeList, fieldNameList, input, isAppend),
                        isAppend);
        replaceTop(project);
        return this;
    }

    /**
     * derive {@code RelDataType} of project operators
     * @param nodeList
     * @param aliasList
     * @param input
     * @param isAppend
     * @return
     */
    private RelDataType deriveType(
            List<RexNode> nodeList,
            List<String> aliasList,
            @Nullable RelNode input,
            boolean isAppend) {
        assert nodeList.size() == aliasList.size();
        List<RelDataTypeField> fields = new ArrayList<>();
        // we need keep all fields of the input node if append is true
        if (isAppend && input != null) {
            fields.addAll(input.getRowType().getFieldList());
        }
        for (int i = 0; i < aliasList.size(); ++i) {
            String aliasName = aliasList.get(i);
            int aliasId =
                    ((GraphOptCluster) getCluster()).getIdGenerator().generate(aliasName, input);
            fields.add(new RelDataTypeFieldImpl(aliasName, aliasId, nodeList.get(i).getType()));
        }
        return new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
    }

    // build group keys

    // global key, i.e. g.V().count()
    public GroupKey groupKey() {
        return groupKey_(ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public GroupKey groupKey(RexNode... variables) {
        return groupKey_(ImmutableList.copyOf(variables), ImmutableList.of());
    }

    @Override
    public GroupKey groupKey(Iterable<? extends RexNode> variables) {
        return groupKey_(ImmutableList.copyOf(variables), ImmutableList.of());
    }

    /**
     * @param variables keys to group by, complex expressions (i.e. "a.age + 1") should be projected in advance
     * @param aliases
     * @return
     */
    public GroupKey groupKey_(List<RexNode> variables, List<@Nullable String> aliases) {
        return new GraphGroupKeys(ImmutableList.copyOf(variables), ImmutableList.copyOf(aliases));
    }

    // build aggregate functions

    /**
     * @param distinct
     * @param alias
     * @param operands keys to aggregate on, complex expressions (i.e. "a.age + 1") should be projected in advance
     * @return
     */
    public AggCall collect(boolean distinct, @Nullable String alias, RexNode... operands) {
        return null;
    }

    @Override
    protected AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            boolean ignoreNulls,
            @Nullable RexNode filter,
            @Nullable ImmutableList<RexNode> distinctKeys,
            ImmutableList<RexNode> orderKeys,
            @Nullable String alias,
            ImmutableList<RexNode> operands) {
        return new GraphAggCall(getCluster(), aggFunction, distinct, alias, operands);
    }

    @Override
    public GraphBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
        Objects.requireNonNull(groupKey);
        ObjectUtils.requireNonEmpty(aggCalls);
        List<GraphAggCall> aggCallList = new ArrayList<>();
        for (AggCall aggCall : aggCalls) {
            aggCallList.add((GraphAggCall) aggCall);
        }
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        RelNode aggregate =
                GraphLogicalAggregate.create(
                        (GraphOptCluster) this.getCluster(),
                        ImmutableList.of(),
                        input,
                        (GraphGroupKeys) groupKey,
                        aggCallList);
        replaceTop(aggregate);
        return this;
    }

    /**
     * build algebra structures for order or limit
     * @param offsetNode
     * @param fetchNode
     * @param nodes build limit() if empty
     * @return
     */
    @Override
    public RelBuilder sortLimit(
            @Nullable RexNode offsetNode,
            @Nullable RexNode fetchNode,
            Iterable<? extends RexNode> nodes) {
        if (offsetNode != null && !(offsetNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("OFFSET node must be RexLiteral");
        }
        if (offsetNode != null && !(offsetNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("FETCH node must be RexLiteral");
        }

        List<RelFieldCollation> fieldCollations = fieldCollations(nodes);
        Config config = ClassUtils.getFieldValue(RelBuilder.class, this, "config");

        // limit 0 -> return empty value
        if ((fetchNode != null && RexLiteral.intValue(fetchNode) == 0) && config.simplifyLimit()) {
            return empty();
        }

        // output all results without any order -> skip
        if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
            return this; // sort is trivial
        }

        RelNode input = requireNonNull(peek(), "frame stack is empty");
        // sortLimit is actually limit if collations are empty
        if (fieldCollations.isEmpty()) {
            // fuse limit with the previous sort operator
            // order + limit -> topK
            if (input instanceof Sort) {
                Sort sort2 = (Sort) input;
                // output all results without any limitations
                if (sort2.offset == null && sort2.fetch == null) {
                    RelNode sort =
                            GraphLogicalSort.create(
                                    sort2.getInput(), sort2.collation, offsetNode, fetchNode);
                    replaceTop(sort);
                    return this;
                }
            }
            // order + project + limit -> topK + project
            if (input instanceof Project) {
                Project project = (Project) input;
                if (project.getInput() instanceof Sort) {
                    Sort sort2 = (Sort) project.getInput();
                    if (sort2.offset == null && sort2.fetch == null) {
                        RelNode sort =
                                GraphLogicalSort.create(
                                        sort2.getInput(), sort2.collation, offsetNode, fetchNode);
                        replaceTop(
                                GraphLogicalProject.create(
                                        (GraphOptCluster) project.getCluster(),
                                        project.getHints(),
                                        sort,
                                        project.getProjects(),
                                        project.getRowType(),
                                        ((GraphLogicalProject) project).isAppend()));
                        return this;
                    }
                }
            }
        }
        RelNode sort =
                GraphLogicalSort.create(
                        input, GraphRelCollations.of(fieldCollations), offsetNode, fetchNode);
        replaceTop(sort);
        return this;
    }

    /**
     * create a list of {@code RelFieldCollation} by order keys
     * @param nodes keys to order by
     * @return
     */
    private List<RelFieldCollation> fieldCollations(Iterable<? extends RexNode> nodes) {
        Objects.requireNonNull(nodes);
        Iterator<? extends RexNode> iterator = nodes.iterator();
        List<RelFieldCollation> collations = new ArrayList<>();
        while (iterator.hasNext()) {
            collations.add(fieldCollation(iterator.next(), RelFieldCollation.Direction.ASCENDING));
        }
        return collations;
    }

    /**
     * create {@code RelFieldCollation} for each order key
     * @param node
     * @param direction
     * @return
     */
    private RelFieldCollation fieldCollation(RexNode node, RelFieldCollation.Direction direction) {
        if (node instanceof RexGraphVariable) {
            return new GraphFieldCollation((RexGraphVariable) node, direction);
        }
        switch (node.getKind()) {
            case DESCENDING:
                return fieldCollation(
                        ((RexCall) node).getOperands().get(0),
                        RelFieldCollation.Direction.DESCENDING);
            default:
                throw new UnsupportedOperationException(
                        "type " + node.getType() + " can not be converted to collation");
        }
    }

    protected void pop() {
        this.build();
    }

    protected void replaceTop(RelNode node) {
        pop();
        push(node);
    }
}
