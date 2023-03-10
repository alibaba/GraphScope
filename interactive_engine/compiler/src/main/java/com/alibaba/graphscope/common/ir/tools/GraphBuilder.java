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

package com.alibaba.graphscope.common.ir.tools;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasChecker;
import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.schema.StatisticSchema;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

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
     * validate and build an algebra structure of {@code GraphLogicalSource}.
     *
     * how to validate:
     * 1. validate the existence of the given labels in config,
     * if exist then derive the {@code GraphSchemaType} of the given labels and keep the type in {@link RelNode#getRowType()},
     * otherwise throw exceptions
     *
     * 2. validate the existence of the given alias in config, if exist throw exceptions
     *
     * @param config
     * @return
     */
    public GraphBuilder source(SourceConfig config) {
        String aliasName = AliasInference.inferDefault(config.getAlias(), new HashSet<>());
        RelNode source =
                GraphLogicalSource.create(
                        (GraphOptCluster) cluster,
                        ImmutableList.of(),
                        config.getOpt(),
                        getTableConfig(config.getLabels(), config.getOpt()),
                        aliasName);
        push(source);
        return this;
    }

    /**
     * validate and build an algebra structure of {@code GraphLogicalExpand}
     *
     * @param config
     * @return
     */
    public GraphBuilder expand(ExpandConfig config) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        RelNode expand =
                GraphLogicalExpand.create(
                        (GraphOptCluster) cluster,
                        ImmutableList.of(),
                        input,
                        config.getOpt(),
                        getTableConfig(config.getLabels(), GraphOpt.Source.EDGE),
                        config.getAlias());
        replaceTop(expand);
        return this;
    }

    /**
     * validate and build an algebra structure of {@code GraphLogicalGetV}
     *
     * @param config
     * @return
     */
    public GraphBuilder getV(GetVConfig config) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        RelNode getV =
                GraphLogicalGetV.create(
                        (GraphOptCluster) cluster,
                        ImmutableList.of(),
                        input,
                        config.getOpt(),
                        getTableConfig(config.getLabels(), GraphOpt.Source.VERTEX),
                        config.getAlias());
        replaceTop(getV);
        return this;
    }

    /**
     * build an algebra structure of {@code GraphLogicalPathExpand}
     *
     * @param pxdConfig
     * @return
     */
    public GraphBuilder pathExpand(PathExpandConfig pxdConfig) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");

        RexNode offsetNode = pxdConfig.getOffset() <= 0 ? null : literal(pxdConfig.getOffset());
        RexNode fetchNode = pxdConfig.getFetch() < 0 ? null : literal(pxdConfig.getFetch());

        Config config = Utils.getFieldValue(RelBuilder.class, this, "config");
        // fetch == 0 -> return empty value
        if ((fetchNode != null && RexLiteral.intValue(fetchNode) == 0) && config.simplifyLimit()) {
            return (GraphBuilder) empty();
        }

        RelNode expand = Objects.requireNonNull(pxdConfig.getExpand());
        RelNode getV = Objects.requireNonNull(pxdConfig.getGetV());
        RelNode pathExpand =
                GraphLogicalPathExpand.create(
                        (GraphOptCluster) cluster,
                        ImmutableList.of(),
                        input,
                        expand,
                        getV,
                        offsetNode,
                        fetchNode,
                        pxdConfig.getResultOpt(),
                        pxdConfig.getPathOpt(),
                        pxdConfig.getAlias());
        replaceTop(pathExpand);
        return this;
    }

    /**
     * convert user-given config to {@code TableConfig},
     * derive all table types (labels with properties) depending on the user given labels
     *
     * @param labelConfig
     * @return
     */
    public TableConfig getTableConfig(LabelConfig labelConfig, GraphOpt.Source opt) {
        List<RelOptTable> relOptTables = new ArrayList<>();
        if (!labelConfig.isAll()) {
            ObjectUtils.requireNonEmpty(labelConfig.getLabels());
            for (String label : labelConfig.getLabels()) {
                relOptTables.add(relOptSchema.getTableForMember(ImmutableList.of(label)));
            }
        } else if (relOptSchema instanceof GraphOptSchema) { // get all labels
            List<List<String>> allLabels =
                    getTableNames(opt, ((GraphOptSchema) relOptSchema).getRootSchema());
            for (List<String> label : allLabels) {
                relOptTables.add(relOptSchema.getTableForMember(label));
            }
        } else {
            throw new IllegalArgumentException(
                    "cannot infer label types from the query given config");
        }
        return new TableConfig(relOptTables).isAll(labelConfig.isAll());
    }

    /**
     * get all table names for a specific {@code opt} to handle fuzzy conditions, i.e. g.V()
     * @param opt
     * @return
     */
    private List<List<String>> getTableNames(GraphOpt.Source opt, StatisticSchema rootSchema) {
        switch (opt) {
            case VERTEX:
                return rootSchema.getVertexList().stream()
                        .map(k -> ImmutableList.of(k.getLabel()))
                        .collect(Collectors.toList());
            case EDGE:
            default:
                return rootSchema.getEdgeList().stream()
                        .map(k -> ImmutableList.of(k.getLabel()))
                        .collect(Collectors.toList());
        }
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
     * validate and build an algebra structure of {@code GraphLogicalSingleMatch}
     * which wrappers all graph operators in one sentence.
     *
     * how to validate:
     * check the graph pattern (lookup from the graph schema and check whether the links are all valid)
     * denoted by each sentence one by one.
     *
     * @param single single sentence
     * @param opt anti or optional
     */
    public GraphBuilder match(RelNode single, GraphOpt.Match opt) {
        RelNode input = size() > 0 ? peek() : null;
        RelNode match =
                GraphLogicalSingleMatch.create((GraphOptCluster) cluster, null, input, single, opt);
        if (size() > 0) pop();
        push(match);
        return this;
    }

    /**
     * validate and build an algebra structure of {@code GraphLogicalMultiMatch}
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
        String varName = AliasInference.SIMPLE_NAME(alias) + AliasInference.DELIMITER + property;
        RelDataTypeField aliasField = getAliasField(alias);
        if (property.equals(GraphProperty.LEN_KEY)) {
            if (!(aliasField.getType() instanceof ArraySqlType)) {
                throw new ClassCastException(
                        "cannot get property='len' from type class ["
                                + aliasField.getType().getClass()
                                + "], should be ["
                                + ArraySqlType.class
                                + "]");
            } else {
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        new GraphProperty(GraphProperty.Opt.LEN),
                        varName,
                        getTypeFactory().createSqlType(SqlTypeName.INTEGER));
            }
        }
        if (!(aliasField.getType() instanceof GraphSchemaType)) {
            throw new ClassCastException(
                    "cannot get property=['id', 'label', 'all', 'key'] from type class ["
                            + aliasField.getType().getClass()
                            + "], should be ["
                            + GraphOptSchema.class
                            + "]");
        }
        if (property.equals(GraphProperty.LABEL_KEY)) {
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.LABEL),
                    varName,
                    getTypeFactory().createSqlType(SqlTypeName.CHAR));
        } else if (property.equals(GraphProperty.ID_KEY)) {
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.ID),
                    varName,
                    getTypeFactory().createSqlType(SqlTypeName.BIGINT));
        } else if (property.equals(GraphProperty.ALL_KEY)) {
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.ALL),
                    varName,
                    getTypeFactory().createSqlType(SqlTypeName.ANY));
        }
        GraphSchemaType graphType = (GraphSchemaType) aliasField.getType();
        List<String> properties = new ArrayList<>();
        boolean isColumnId =
                (relOptSchema instanceof GraphOptSchema)
                        ? ((GraphOptSchema) relOptSchema).getRootSchema().isColumnId()
                        : false;
        for (RelDataTypeField pField : graphType.getFieldList()) {
            if (pField.getName().equals(property)) {
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        isColumnId
                                ? new GraphProperty(new GraphNameOrId(pField.getIndex()))
                                : new GraphProperty(new GraphNameOrId(pField.getName())),
                        varName,
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
     *
     * @param alias
     * @return
     */
    private RelDataTypeField getAliasField(String alias) {
        Objects.requireNonNull(alias);
        List<String> aliases = new ArrayList<>();
        for (int inputOrdinal = 0; inputOrdinal < size(); ++inputOrdinal) {
            List<RelNode> inputQueue = Lists.newArrayList(peek(inputOrdinal));
            while (!inputQueue.isEmpty()) {
                RelNode cur = inputQueue.remove(0);
                List<RelDataTypeField> fields = cur.getRowType().getFieldList();
                if (alias == AliasInference.DEFAULT_NAME && fields.size() == 1) {
                    return fields.get(0);
                }
                for (RelDataTypeField field : fields) {
                    if (field.getName().equals(alias)) {
                        return field;
                    }
                    aliases.add(AliasInference.SIMPLE_NAME(field.getName()));
                }
                if (AliasInference.removeAlias(cur)) {
                    break;
                }
                inputQueue.addAll(cur.getInputs());
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
        return call_(operator, ImmutableList.copyOf(operands));
    }

    @Override
    public RexNode call(SqlOperator operator, Iterable<? extends RexNode> operands) {
        return call_(operator, ImmutableList.copyOf(operands));
    }

    private RexNode call_(SqlOperator operator, List<RexNode> operandList) {
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
                || sqlKind.belongsTo(SqlKind.COMPARISON)
                || sqlKind == SqlKind.AND
                || sqlKind == SqlKind.OR
                || sqlKind == SqlKind.DESCENDING
                || (sqlKind == SqlKind.OTHER_FUNCTION && operator.getName().equals("POWER"))
                || (sqlKind == SqlKind.MINUS_PREFIX);
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
            // fuze all conditions into table scan
            if (condition.accept(checker)) {
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
        Config config = Utils.getFieldValue(RelBuilder.class, this, "config");
        RexSimplify simplifier = Utils.getFieldValue(RelBuilder.class, this, "simplifier");

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
                        nodeList,
                        fieldNameList,
                        AliasInference.getUniqueAliasList(input, isAppend));
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
     *
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
        for (int i = 0; i < aliasList.size(); ++i) {
            String aliasName = aliasList.get(i);
            fields.add(
                    new RelDataTypeFieldImpl(
                            aliasName,
                            generateAliasId(aliasName, input),
                            nodeList.get(i).getType()));
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

    public GroupKey groupKey(List<RexNode> variables, List<@Nullable String> aliases) {
        return groupKey_(variables, aliases);
    }

    /**
     * @param variables keys to group by, complex expressions (i.e. "a.age + 1") should be projected in advance
     * @param aliases
     * @return
     */
    private GroupKey groupKey_(List<RexNode> variables, List<@Nullable String> aliases) {
        return null;
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
        return null;
    }

    @Override
    public GraphBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
        return this;
    }

    /**
     * build algebra structures for order or limit
     *
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
        return this;
    }

    protected void pop() {
        this.build();
    }

    protected void replaceTop(RelNode node) {
        pop();
        push(node);
    }
}
