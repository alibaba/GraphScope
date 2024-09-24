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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.function.GraphFunctions;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphRelCollations;
import com.alibaba.graphscope.common.ir.rex.*;
import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.ir.type.*;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.proto.frontend.Code;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.GraphInferTypes;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Integrate interfaces to build algebra structures,
 * including {@link RexNode} for expressions and {@link RelNode} for operators
 */
public class GraphBuilder extends RelBuilder {
    private final Configs configs;
    /**
     * @param context      not used currently
     * @param cluster      get {@link org.apache.calcite.rex.RexBuilder} (to build {@code RexNode})
     *                     and other global resources (not used currently) from it
     * @param relOptSchema get graph schema from it
     */
    protected GraphBuilder(Context context, GraphOptCluster cluster, RelOptSchema relOptSchema) {
        super(Objects.requireNonNull(context), cluster, relOptSchema);
        Utils.setFieldValue(
                RelBuilder.class,
                this,
                "simplifier",
                new GraphRexSimplify(
                        cluster.getRexBuilder(), RelOptPredicateList.EMPTY, RexUtil.EXECUTOR));
        this.configs = context.unwrapOrThrow(Configs.class);
    }

    /**
     * @param context
     * @param cluster
     * @param relOptSchema
     * @return
     */
    public static GraphBuilder create(
            Context context, GraphOptCluster cluster, RelOptSchema relOptSchema) {
        return new GraphBuilder(context, cluster, relOptSchema);
    }

    public Context getContext() {
        return this.configs;
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
        RelNode source =
                GraphLogicalSource.create(
                        (GraphOptCluster) cluster,
                        ImmutableList.of(),
                        config.getOpt(),
                        getTableConfig(config.getLabels(), config.getOpt()),
                        config.getAlias());
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
                        config.getAlias(),
                        getAliasNameWithId(
                                config.getStartAlias(),
                                (RelDataType type) ->
                                        (type instanceof GraphSchemaType)
                                                && ((GraphSchemaType) type).getScanOpt()
                                                        == GraphOpt.Source.VERTEX));
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
                        config.getAlias(),
                        getAliasNameWithId(
                                config.getStartAlias(),
                                (RelDataType type) ->
                                        (type instanceof GraphSchemaType)
                                                        && ((GraphSchemaType) type).getScanOpt()
                                                                == GraphOpt.Source.EDGE
                                                || type instanceof GraphPathType));
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
                        pxdConfig.getUntilCondition(),
                        pxdConfig.getAlias(),
                        getAliasNameWithId(
                                pxdConfig.getStartAlias(),
                                (RelDataType type) ->
                                        (type instanceof GraphSchemaType)
                                                && ((GraphSchemaType) type).getScanOpt()
                                                        == GraphOpt.Source.VERTEX));
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
        Preconditions.checkArgument(
                relOptSchema != null, "cannot create table config from the 'null' schema");
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

    private AliasNameWithId getAliasNameWithId(
            @Nullable String aliasName, Predicate<RelDataType> checkType) {
        aliasName =
                AliasInference.isDefaultAlias(aliasName) ? AliasInference.DEFAULT_NAME : aliasName;
        RexGraphVariable variable = variable(aliasName);
        Preconditions.checkArgument(
                checkType.test(variable.getType()),
                "object with tag=%s mismatch with the expected type, current type is %s",
                aliasName,
                variable.getType());
        return new AliasNameWithId(aliasName, variable.getAliasId());
    }

    /**
     * get all table names for a specific {@code opt} to handle fuzzy conditions, i.e. g.V()
     * @param opt
     * @return
     */
    private List<List<String>> getTableNames(GraphOpt.Source opt, IrGraphSchema rootSchema) {
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

    /** f
     * generate a new alias id for the given alias name
     *
     * @param alias
     * @return
     */
    private int generateAliasId(@Nullable String alias) {
        RelOptCluster cluster = getCluster();
        return ((GraphOptCluster) cluster).getIdGenerator().generate(alias);
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
        if (FrontendConfig.GRAPH_TYPE_INFERENCE_ENABLED.get(configs)) {
            single =
                    new GraphTypeInference(
                                    GraphBuilder.create(
                                            this.configs,
                                            (GraphOptCluster) this.cluster,
                                            this.relOptSchema))
                            .inferTypes(single);
        }
        RelNode input = size() > 0 ? peek() : null;
        // unwrap match if there is only one source operator in the sentence
        RelNode match =
                (input == null && single instanceof GraphLogicalSource)
                        ? single
                        : GraphLogicalSingleMatch.create(
                                (GraphOptCluster) cluster,
                                null,
                                null,
                                single,
                                (input == null) ? opt : GraphOpt.Match.INNER);
        if (input == null) {
            push(match);
        } else {
            JoinRelType joinType = getJoinRelType(opt);
            RexNode joinCondition = getJoinCondition(input, match);
            if (joinType == JoinRelType.ANTI) {
                push(match).antiJoin(joinCondition);
            } else {
                push(match).join(joinType, joinCondition);
            }
        }
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
        List<RelNode> sentences = Lists.newArrayList();
        sentences.add(first);
        for (RelNode other : others) {
            sentences.add(other);
        }
        Preconditions.checkArgument(
                sentences.size() > 1, "at least two sentences are required in multiple match");
        if (FrontendConfig.GRAPH_TYPE_INFERENCE_ENABLED.get(configs)) {
            sentences =
                    new GraphTypeInference(
                                    GraphBuilder.create(
                                            this.configs,
                                            (GraphOptCluster) this.cluster,
                                            this.relOptSchema))
                            .inferTypes(sentences);
        }
        RelNode input = size() > 0 ? peek() : null;
        RelNode match =
                GraphLogicalMultiMatch.create(
                        (GraphOptCluster) cluster,
                        null,
                        null,
                        sentences.get(0),
                        sentences.subList(1, sentences.size()));
        if (input == null) {
            push(match);
        } else {
            push(match).join(getJoinRelType(GraphOpt.Match.INNER), getJoinCondition(input, match));
        }
        return this;
    }

    @Override
    public GraphBuilder push(RelNode node) {
        super.push(node);
        return this;
    }

    public RexNode getJoinCondition(RelNode first, RelNode second) {
        List<RexNode> conditions = Lists.newArrayList();
        List<RelDataTypeField> firstFields =
                com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(first).getFieldList();
        List<RelDataTypeField> secondFields =
                com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(second).getFieldList();
        for (RelDataTypeField firstField : firstFields) {
            for (RelDataTypeField secondField : secondFields) {
                if (isGraphElementTypeWithSameOpt(firstField.getType(), secondField.getType())
                        && firstField.getIndex() != AliasInference.DEFAULT_ID
                        && firstField.getIndex() == secondField.getIndex()
                        && firstField.getName().equals(secondField.getName())) {
                    RexGraphVariable leftKey =
                            RexGraphVariable.of(
                                    firstField.getIndex(),
                                    getColumnIndex(first, firstField),
                                    AliasInference.SIMPLE_NAME(firstField.getName()),
                                    firstField.getType());
                    RexGraphVariable rightKey =
                            RexGraphVariable.of(
                                    secondField.getIndex(),
                                    firstFields.size() + getColumnIndex(second, secondField),
                                    AliasInference.SIMPLE_NAME(secondField.getName()),
                                    secondField.getType());
                    conditions.add(equals(leftKey, rightKey));
                }
            }
        }
        return and(conditions);
    }

    private boolean isGraphElementTypeWithSameOpt(RelDataType first, RelDataType second) {
        return first instanceof GraphSchemaType
                && second instanceof GraphSchemaType
                && ((GraphSchemaType) first).getScanOpt()
                        == ((GraphSchemaType) second).getScanOpt();
    }

    private JoinRelType getJoinRelType(GraphOpt.Match opt) {
        switch (opt) {
            case OPTIONAL:
                return JoinRelType.LEFT;
            case ANTI:
                return JoinRelType.ANTI;
            default:
                return JoinRelType.INNER;
        }
    }

    /**
     * validate and build {@link RexGraphVariable} from a given alias (i.e. "a")
     *
     * @param alias
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias) {
        alias = AliasInference.isDefaultAlias(alias) ? AliasInference.DEFAULT_NAME : alias;
        List<ColumnField> columnFields = getAliasField(alias);
        if (columnFields.size() == 1) {
            ColumnField columnField = columnFields.get(0);
            RelDataTypeField aliasField = columnField.right;
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    columnField.left,
                    aliasField.getName(),
                    aliasField.getType());
        } else {
            return RexGraphVariableList.of(
                    columnFields.stream()
                            .map(
                                    field ->
                                            RexGraphVariable.of(
                                                    field.right.getIndex(),
                                                    field.left,
                                                    AliasInference.SIMPLE_NAME(
                                                            field.right.getName()),
                                                    field.right.getType()))
                            .collect(Collectors.toList()));
        }
    }

    /**
     * validate and build {@link RexGraphVariable} from a given variable containing fieldName (i.e. "a.name" or "name")
     *
     * @param alias
     * @param property
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias, String property) {
        alias = AliasInference.isDefaultAlias(alias) ? AliasInference.DEFAULT_NAME : alias;
        Objects.requireNonNull(property);
        String varName = AliasInference.SIMPLE_NAME(alias) + AliasInference.DELIMITER + property;
        List<ColumnField> columnFields = getAliasField(alias);
        if (columnFields.size() != 1) {
            throw new FrontendException(
                    Code.PROPERTY_NOT_FOUND,
                    "cannot get property="
                            + property
                            + " from alias="
                            + alias
                            + ", expected one column, but found "
                            + columnFields.size());
        }
        ColumnField columnField = columnFields.get(0);
        RelDataTypeField aliasField = columnField.right;
        if (property.equals(GraphProperty.LEN_KEY)) {
            if (!(aliasField.getType() instanceof GraphPathType)) {
                throw new FrontendException(
                        Code.PROPERTY_NOT_FOUND,
                        "cannot get property='len' from type class ["
                                + aliasField.getType().getClass()
                                + "], should be ["
                                + GraphPathType.class
                                + "]");
            } else {
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        new GraphProperty(GraphProperty.Opt.LEN),
                        columnField.left,
                        varName,
                        getTypeFactory().createSqlType(SqlTypeName.INTEGER));
            }
        }
        if (!(aliasField.getType() instanceof GraphSchemaType)) {
            throw new FrontendException(
                    Code.PROPERTY_NOT_FOUND,
                    "cannot get property=['id', 'label', 'all', 'key'] from type class ["
                            + aliasField.getType().getClass()
                            + "], should be ["
                            + GraphSchemaType.class
                            + "]");
        }
        if (property.equals(GraphProperty.LABEL_KEY)) {
            GraphSchemaType schemaType = (GraphSchemaType) aliasField.getType();
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.LABEL),
                    columnField.left,
                    varName,
                    schemaType.getLabelType());
        } else if (property.equals(GraphProperty.ID_KEY)) {
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.ID),
                    columnField.left,
                    varName,
                    getTypeFactory().createSqlType(SqlTypeName.BIGINT));
        } else if (property.equals(GraphProperty.ALL_KEY)) {
            return RexGraphVariable.of(
                    aliasField.getIndex(),
                    new GraphProperty(GraphProperty.Opt.ALL),
                    columnField.left,
                    varName,
                    getTypeFactory().createSqlType(SqlTypeName.ANY));
        } else if (property.equals(GraphProperty.START_V_KEY)) {
            if (!(aliasField.getType() instanceof GraphPathType)) {
                throw new FrontendException(
                        Code.PROPERTY_NOT_FOUND,
                        "cannot get property='start_v' from type class ["
                                + aliasField.getType().getClass()
                                + "], should be ["
                                + GraphPathType.class
                                + "]");
            } else {
                Preconditions.checkArgument(size() > 0, "frame stack is empty");
                RelNode peek = peek();
                Preconditions.checkArgument(
                        peek != null && !peek.getInputs().isEmpty(),
                        "path expand should have start vertex");
                RelNode input = peek.getInput(0);
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        new GraphProperty(GraphProperty.Opt.START_V),
                        columnField.left,
                        varName,
                        input.getRowType().getFieldList().get(0).getType());
            }
        } else if (property.equals(GraphProperty.END_V_KEY)) {
            if (!(aliasField.getType() instanceof GraphPathType)) {
                throw new FrontendException(
                        Code.PROPERTY_NOT_FOUND,
                        "cannot get property='end_v' from type class ["
                                + aliasField.getType().getClass()
                                + "], should be ["
                                + GraphPathType.class
                                + "]");
            } else {
                GraphPathType pathType = (GraphPathType) aliasField.getType();
                return RexGraphVariable.of(
                        aliasField.getIndex(),
                        new GraphProperty(GraphProperty.Opt.END_V),
                        columnField.left,
                        varName,
                        pathType.getComponentType().getGetVType());
            }
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
                        columnField.left,
                        varName,
                        pField.getType());
            }
            properties.add(pField.getName());
        }
        throw new FrontendException(
                Code.PROPERTY_NOT_FOUND,
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
    private List<ColumnField> getAliasField(String alias) {
        Objects.requireNonNull(alias);
        if (alias.equals(AliasInference.STAR)) {
            RelNode peek = requireNonNull(peek(), "frame stack is empty");
            RelDataType outputType =
                    com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(peek);
            return outputType.getFieldList().stream()
                    .map(
                            field -> {
                                int columnIdx =
                                        AliasInference.isDefaultAlias(field.getName())
                                                ? AliasInference.DEFAULT_COLUMN_ID
                                                : getColumnIndex(peek, field);
                                return new ColumnField(columnIdx, field);
                            })
                    .collect(Collectors.toList());
        }
        Set<String> aliases = new HashSet<>();
        int nodeIdx = 0;
        for (int inputOrdinal = 0; inputOrdinal < size(); ++inputOrdinal) {
            List<RelNode> inputQueue = Lists.newArrayList(peek(inputOrdinal));
            while (!inputQueue.isEmpty()) {
                RelNode cur = inputQueue.remove(0);
                List<RelDataTypeField> fields = cur.getRowType().getFieldList();
                // to support `head` in gremlin
                if (nodeIdx++ == 0 && AliasInference.isDefaultAlias(alias)) {
                    if (fields.size() == 1) {
                        return ImmutableList.of(
                                new ColumnField(
                                        AliasInference.DEFAULT_COLUMN_ID,
                                        new RelDataTypeFieldImpl(
                                                AliasInference.DEFAULT_NAME,
                                                AliasInference.DEFAULT_ID,
                                                fields.get(0).getType())));
                    } else if (cur
                            instanceof
                            CommonTableScan) { // specific implementation for gremlin, to get `head`
                        // in nested traversal
                        return ImmutableList.of(
                                new ColumnField(
                                        AliasInference.DEFAULT_COLUMN_ID,
                                        new RelDataTypeFieldImpl(
                                                AliasInference.DEFAULT_NAME,
                                                AliasInference.DEFAULT_ID,
                                                fields.get(fields.size() - 1).getType())));
                    } else { // If the output of the current operator consists of multiple columns,
                        // we transform `head` into all columns of the current operator.
                        return fields.stream()
                                .map(field -> new ColumnField(getColumnIndex(cur, field), field))
                                .collect(Collectors.toList());
                    }
                }
                for (RelDataTypeField field : fields) {
                    if (!AliasInference.isDefaultAlias(alias) && field.getName().equals(alias)) {
                        return ImmutableList.of(new ColumnField(getColumnIndex(cur, field), field));
                    }
                    aliases.add(AliasInference.SIMPLE_NAME(field.getName()));
                }
                if (AliasInference.removeAlias(cur)) {
                    break;
                }
                inputQueue.addAll(cur.getInputs());
            }
        }
        throw new FrontendException(
                Code.TAG_NOT_FOUND,
                "{alias="
                        + AliasInference.SIMPLE_NAME(alias)
                        + "} "
                        + "not found; expected aliases are: "
                        + aliases);
    }

    private static class ColumnField extends Pair<Integer, RelDataTypeField> {
        public ColumnField(Integer left, RelDataTypeField right) {
            super(left, right);
        }
    }

    private int getColumnIndex(RelNode node, RelDataTypeField field) {
        Set<String> uniqueFieldNames = Sets.newHashSet();
        if (!visitField(node, field, uniqueFieldNames)) {
            throw new IllegalArgumentException("field " + field + " not found in node" + node);
        }
        return uniqueFieldNames.size();
    }

    // find column index of the target field in recursive way
    // i.e. (a)-[b]->(c) -> a:0, b:1, c:2
    private boolean visitField(
            RelNode topNode, RelDataTypeField targetField, Set<String> uniqueFieldNames) {
        if (!(AliasInference.removeAlias(topNode)
                || topNode instanceof Join
                || topNode instanceof AbstractLogicalMatch)) {
            for (RelNode child : topNode.getInputs()) {
                if (visitField(child, targetField, uniqueFieldNames)) {
                    return true;
                }
            }
        }
        List<RelDataTypeField> fields = topNode.getRowType().getFieldList();
        for (RelDataTypeField field : fields) {
            if (!AliasInference.isDefaultAlias(field.getName()) && field.equals(targetField)) {
                return true;
            } else if (!AliasInference.isDefaultAlias(field.getName())
                    && !uniqueFieldNames.contains(field.getName())) {
                uniqueFieldNames.add(field.getName());
            }
        }
        return false;
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

    public RexNode procedureCall(
            SqlOperator operator,
            Iterable<? extends RexNode> operands,
            StoredProcedureMeta procedureMeta) {
        RexCall call = (RexCall) call(operator, operands);
        return new RexProcedureCall(
                call.getType(), call.getOperator(), call.getOperands(), procedureMeta);
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
        // infer unknown operands types from other known types
        if (operator.getOperandTypeInference() != GraphInferTypes.RETURN_TYPE) {
            operandList =
                    inferOperandTypes(operator, getTypeFactory().createUnknownType(), operandList);
        }
        RexCallBinding callBinding =
                new RexCallBinding(getTypeFactory(), operator, operandList, ImmutableList.of());
        // check count of operands, if fail throw exceptions
        operator.validRexOperands(callBinding.getOperandCount(), Litmus.THROW);
        // check type of each operand, if fail throw exceptions
        operator.checkOperandTypes(callBinding, true);
        // derive return type
        RelDataType returnType = operator.inferReturnType(callBinding);
        // derive unknown types of operands
        operandList =
                inferOperandTypes(operator, returnType, convertOperands(operator, operandList));
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if (operator.getKind() == SqlKind.OTHER && operator.getName().equals("IN")) {
            return rexBuilder.makeIn(
                    operandList.get(0), operandList.subList(1, operandList.size()));
        }
        return rexBuilder.makeCall(returnType, operator, operandList);
    }

    // convert operands of the operator in some special cases
    private List<RexNode> convertOperands(SqlOperator operator, List<RexNode> operandList) {
        if (operator.getKind() == SqlKind.EXTRACT) {
            RexNode intervalOperand = operandList.get(0);
            if (intervalOperand instanceof RexLiteral
                    && ((RexLiteral) intervalOperand).isNull()
                    && intervalOperand.getType() instanceof IntervalSqlType) {
                IntervalSqlType intervalType = (IntervalSqlType) intervalOperand.getType();
                List<RexNode> newOperands = Lists.newArrayList();
                newOperands.add(
                        getRexBuilder()
                                .makeFlag(intervalType.getIntervalQualifier().getStartUnit()));
                newOperands.add(operandList.get(1));
                return newOperands;
            }
        }
        // todo: convert label names to ids
        return operandList;
    }

    private List<RexNode> inferOperandTypes(
            SqlOperator operator, RelDataType returnType, List<RexNode> operandList) {
        if (operator.getOperandTypeInference() != null
                && operandList.stream()
                        .anyMatch((t) -> t.getType().getSqlTypeName() == SqlTypeName.UNKNOWN)) {
            RexCallBinding callBinding =
                    new RexCallBinding(getTypeFactory(), operator, operandList, ImmutableList.of());
            RelDataType[] newTypes = callBinding.collectOperandTypes().toArray(new RelDataType[0]);
            operator.getOperandTypeInference().inferOperandTypes(callBinding, returnType, newTypes);
            List<RexNode> typeInferredOperands = new ArrayList<>(operandList.size());
            GraphRexBuilder rexBuilder = (GraphRexBuilder) this.getRexBuilder();
            for (int i = 0; i < operandList.size(); ++i) {
                RexNode rexNode = operandList.get(i);
                if (rexNode instanceof RexGraphDynamicParam) {
                    RexGraphDynamicParam graphDynamicParam = (RexGraphDynamicParam) rexNode;
                    rexNode =
                            rexBuilder.makeGraphDynamicParam(
                                    newTypes[i],
                                    graphDynamicParam.getName(),
                                    graphDynamicParam.getIndex());
                }
                typeInferredOperands.add(rexNode);
            }
            return typeInferredOperands;
        } else {
            return operandList;
        }
    }

    private boolean isCurrentSupported(SqlOperator operator) {
        SqlKind sqlKind = operator.getKind();
        return sqlKind.belongsTo(SqlKind.BINARY_ARITHMETIC)
                || sqlKind.belongsTo(SqlKind.COMPARISON)
                || sqlKind == SqlKind.AND
                || sqlKind == SqlKind.OR
                || sqlKind == SqlKind.DESCENDING
                || (sqlKind == SqlKind.OTHER_FUNCTION
                        && (operator.getName().equals("POWER")
                                || operator.getName().equals("<<")
                                || operator.getName().equals(">>")))
                || (sqlKind == SqlKind.MINUS_PREFIX)
                || (sqlKind == SqlKind.CASE)
                || (sqlKind == SqlKind.PROCEDURE_CALL)
                || (sqlKind == SqlKind.NOT)
                || sqlKind == SqlKind.ARRAY_VALUE_CONSTRUCTOR
                || sqlKind == SqlKind.MAP_VALUE_CONSTRUCTOR
                || sqlKind == SqlKind.IS_NULL
                || sqlKind == SqlKind.IS_NOT_NULL
                || sqlKind == SqlKind.EXTRACT
                || sqlKind == SqlKind.SEARCH
                || sqlKind == SqlKind.POSIX_REGEX_CASE_SENSITIVE
                || sqlKind == SqlKind.AS
                || sqlKind == SqlKind.BIT_AND
                || sqlKind == SqlKind.BIT_OR
                || sqlKind == SqlKind.BIT_XOR
                || (sqlKind == SqlKind.OTHER
                                && (operator.getName().equals("IN")
                                        || operator.getName().equals("DATETIME_MINUS")
                                        || operator.getName().equals("PATH_CONCAT")
                                        || operator.getName().equals("PATH_FUNCTION"))
                        || operator.getName().startsWith(GraphFunctions.FUNCTION_PREFIX))
                || sqlKind == SqlKind.ARRAY_CONCAT;
    }

    @Override
    public GraphBuilder filter(RexNode... conditions) {
        return filter(ImmutableList.copyOf(conditions));
    }

    @Override
    public GraphBuilder filter(Iterable<? extends RexNode> conditions) {
        conditions = flatExprs((Iterable<RexNode>) conditions);

        RexVisitor propertyChecker = new RexPropertyChecker(true, this);
        for (RexNode condition : conditions) {
            RelDataType type = condition.getType();
            // make sure all conditions have the Boolean return type
            if (!(type instanceof BasicSqlType) || type.getSqlTypeName() != SqlTypeName.BOOLEAN) {
                throw new IllegalArgumentException(
                        "filter condition "
                                + condition
                                + " should return Boolean value, but is "
                                + type);
            }
            // check property existence for specific label
            condition.accept(propertyChecker);
        }

        // specific implementation for gremlin, project will change the 'head' before the current
        // filter, which need to be recovered later
        RelDataTypeField recoverHead = null;

        RexSubQueryPreComputer preComputer = new RexSubQueryPreComputer(this);
        List<RexNode> newConditions = Lists.newArrayList();
        for (RexNode condition : conditions) {
            // if the condition has subquery as its operand, i.e. where(out().out().count() > 2),
            // subquery(out().out().count()) needs to be precomputed in advance, the condition also
            // needs to be adapted
            newConditions.add(preComputer.precompute(condition));
        }
        // project subquery in advance
        if (!preComputer.getSubQueryNodes().isEmpty()) {
            RelNode input = requireNonNull(peek(), "frame stack is empty");
            if (input.getRowType().getFieldList().size() == 1) {
                RelDataTypeField field = input.getRowType().getFieldList().get(0);
                // give a non-default alias to the head, so that it can be recovered later
                if (field.getName() == AliasInference.DEFAULT_NAME) {
                    Set<String> uniqueAliases = AliasInference.getUniqueAliasList(input, true);
                    uniqueAliases.addAll(preComputer.getSubQueryAliases());
                    String nonDefault = AliasInference.inferAliasWithPrefix("$f", uniqueAliases);
                    // set the non default alias to the input
                    as(nonDefault);
                    recoverHead =
                            new RelDataTypeFieldImpl(
                                    nonDefault, generateAliasId(nonDefault), field.getType());
                } else {
                    recoverHead = field;
                }
            }
            project(preComputer.getSubQueryNodes(), preComputer.getSubQueryAliases(), true);
            conditions =
                    newConditions.stream()
                            .map(k -> k.accept(new RexTmpVariableConverter(true, this)))
                            .collect(Collectors.toList());
        }
        super.filter(ImmutableSet.of(), conditions);
        // fuse filter with the previous table scan if meets the conditions
        Filter filter = topFilter();
        if (filter != null) {
            GraphBuilder builder =
                    GraphBuilder.create(
                            this.configs, (GraphOptCluster) getCluster(), getRelOptSchema());
            RexNode condition = filter.getCondition();
            RelNode input = !filter.getInputs().isEmpty() ? filter.getInput(0) : null;
            if (input instanceof AbstractBindableTableScan) {
                AbstractBindableTableScan tableScan = (AbstractBindableTableScan) input;
                List<Integer> aliasIds =
                        condition.accept(
                                new RexVariableAliasCollector<>(
                                        true, RexGraphVariable::getAliasId));
                // fuze all conditions into table scan
                if (!aliasIds.isEmpty()
                        && ImmutableList.of(AliasInference.DEFAULT_ID, tableScan.getAliasId())
                                .containsAll(aliasIds)) {
                    condition =
                            condition.accept(
                                    new RexVariableAliasConverter(
                                            true,
                                            this,
                                            AliasInference.SIMPLE_NAME(AliasInference.DEFAULT_NAME),
                                            AliasInference.DEFAULT_ID));
                    // add condition into table scan
                    // pop the filter from the inner stack
                    replaceTop(fuseFilters(tableScan, condition, builder));
                }
            } else if (input instanceof AbstractLogicalMatch) {
                List<RexNode> extraFilters = Lists.newArrayList();
                AbstractLogicalMatch match =
                        fuseFilters((AbstractLogicalMatch) input, condition, extraFilters, builder);
                if (!match.equals(input)) {
                    if (extraFilters.isEmpty()) {
                        replaceTop(match);
                    } else {
                        replaceTop(builder.push(match).filter(extraFilters).build());
                    }
                }
            }
        }
        if (recoverHead != null) {
            project(ImmutableList.of(variable(recoverHead.getName())), ImmutableList.of(), true);
        }
        return this;
    }

    private AbstractBindableTableScan fuseFilters(
            AbstractBindableTableScan tableScan, RexNode condition, GraphBuilder builder) {
        RexFilterClassifier classifier = new RexFilterClassifier(builder, tableScan);
        ClassifiedFilter filterResult = classifier.classify(condition);
        List<Comparable> labelValues = filterResult.getLabelValues();
        List<RexNode> uniqueKeyFilters = Lists.newArrayList(filterResult.getUniqueKeyFilters());
        List<RexNode> extraFilters = Lists.newArrayList(filterResult.getExtraFilters());
        if (!labelValues.isEmpty()) {
            GraphLabelType labelType =
                    ((GraphSchemaType) tableScan.getRowType().getFieldList().get(0).getType())
                            .getLabelType();
            List<String> labelsToKeep =
                    labelType.getLabelsEntry().stream()
                            .filter(k -> labelValues.contains(k.getLabel()))
                            .map(k -> k.getLabel())
                            .collect(Collectors.toList());
            Preconditions.checkArgument(
                    !labelsToKeep.isEmpty(),
                    "cannot find common labels between values= " + labelValues + " and label=",
                    labelType);
            if (labelsToKeep.size() < labelType.getLabelsEntry().size()) {
                LabelConfig newLabelConfig = new LabelConfig(false);
                labelsToKeep.forEach(k -> newLabelConfig.addLabel(k));
                if (tableScan instanceof GraphLogicalSource) {
                    builder.source(
                            new SourceConfig(
                                    ((GraphLogicalSource) tableScan).getOpt(),
                                    newLabelConfig,
                                    tableScan.getAliasName()));
                } else if (tableScan instanceof GraphLogicalExpand) {
                    builder.push(tableScan.getInput(0))
                            .expand(
                                    new ExpandConfig(
                                            ((GraphLogicalExpand) tableScan).getOpt(),
                                            newLabelConfig,
                                            tableScan.getAliasName()));
                } else if (tableScan instanceof GraphLogicalGetV) {
                    builder.push(tableScan.getInput(0))
                            .getV(
                                    new GetVConfig(
                                            ((GraphLogicalGetV) tableScan).getOpt(),
                                            newLabelConfig,
                                            tableScan.getAliasName()));
                }
                if (builder.size() > 0) {
                    // check if the property still exist after updating the label type
                    RexVisitor propertyChecker = new RexPropertyChecker(true, builder);
                    if (tableScan instanceof GraphLogicalSource) {
                        RexNode originalUniqueKeyFilters =
                                ((GraphLogicalSource) tableScan).getUniqueKeyFilters();
                        if (originalUniqueKeyFilters != null) {
                            originalUniqueKeyFilters.accept(propertyChecker);
                            builder.filter(originalUniqueKeyFilters);
                        }
                        if (!uniqueKeyFilters.isEmpty()) {
                            builder.filter(uniqueKeyFilters);
                            uniqueKeyFilters.clear();
                        }
                    }
                    ImmutableList originalFilters = tableScan.getFilters();
                    if (ObjectUtils.isNotEmpty(originalFilters)) {
                        originalFilters.forEach(k -> ((RexNode) k).accept(propertyChecker));
                        builder.filter(originalFilters);
                    }
                    if (!extraFilters.isEmpty()) {
                        extraFilters.forEach(k -> k.accept(propertyChecker));
                        builder.filter(extraFilters);
                        extraFilters.clear();
                    }
                    tableScan = (AbstractBindableTableScan) builder.build();
                }
            }
        }
        if (tableScan instanceof GraphLogicalSource && !uniqueKeyFilters.isEmpty()) {
            GraphLogicalSource source = (GraphLogicalSource) tableScan;
            if (source.getUniqueKeyFilters() != null || uniqueKeyFilters.size() > 1) {
                extraFilters.addAll(uniqueKeyFilters);
            } else {
                source.setUniqueKeyFilters(uniqueKeyFilters.get(0));
            }
        }
        if (!extraFilters.isEmpty()) {
            ImmutableList originalFilters = tableScan.getFilters();
            if (ObjectUtils.isNotEmpty(originalFilters)) {
                for (int i = 0; i < originalFilters.size(); ++i) {
                    extraFilters.add(i, (RexNode) originalFilters.get(i));
                }
            }
            tableScan.setFilters(
                    ImmutableList.of(
                            RexUtil.composeConjunction(this.getRexBuilder(), extraFilters)));
        }
        return tableScan;
    }

    /**
     * fuse label filters into the {@code match} if possible
     * @param match
     * @param condition
     * @param extraFilters
     * @param builder
     * @return
     */
    private AbstractLogicalMatch fuseFilters(
            AbstractLogicalMatch match,
            RexNode condition,
            List<RexNode> extraFilters,
            GraphBuilder builder) {
        RexFilterClassifier classifier = new RexFilterClassifier(builder, null);
        ClassifiedFilter filter = classifier.classify(condition);
        List<RexNode> labelFilters = filter.getLabelFilters();
        extraFilters.addAll(filter.getExtraFilters());
        for (RexNode labelFilter : labelFilters) {
            PushFilterVisitor visitor = new PushFilterVisitor(builder, labelFilter);
            match = (AbstractLogicalMatch) match.accept(visitor);
            if (!visitor.isPushed()) {
                extraFilters.add(labelFilter);
            }
        }
        return match;
    }

    // return the top node if its type is Filter, otherwise null
    private Filter topFilter() {
        if (this.size() > 0 && this.peek() instanceof Filter) {
            return (Filter) this.peek();
        } else {
            return null;
        }
    }

    public GraphBuilder project(RexNode... nodes) {
        return project(ImmutableList.copyOf(nodes));
    }

    @Override
    public GraphBuilder project(Iterable<? extends RexNode> nodes) {
        return project(nodes, ImmutableList.of());
    }

    public GraphBuilder project(
            Iterable<? extends RexNode> nodes, Iterable<? extends @Nullable String> fieldNames) {
        return project(nodes, fieldNames, false);
    }

    @Override
    public GraphBuilder project(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> aliases,
            boolean isAppend) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        Config config = Utils.getFieldValue(RelBuilder.class, this, "config");
        RexSimplify simplifier = Utils.getFieldValue(RelBuilder.class, this, "simplifier");

        nodes = flatExprs((Iterable<RexNode>) nodes);

        List<RexNode> nodeList = Lists.newArrayList(nodes);
        List<@Nullable String> fieldNameList = Lists.newArrayList(aliases);

        // Simplify expressions.
        if (config.simplify()) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.set(i, simplifier.simplifyPreservingType(nodeList.get(i)));
            }
        }

        RexSubQueryPreComputer preComputer = new RexSubQueryPreComputer(this);
        List<RexNode> newNodeList = Lists.newArrayList();
        for (RexNode node : nodeList) {
            // if the node has subquery as its operand, i.e. select('a').by(out().out().count()),
            // subquery(out().out().count()) needs to be precomputed in advance, the node also needs
            // to be adapted
            newNodeList.add(preComputer.precompute(node));
        }
        // project subquery in advance
        if (!preComputer.getSubQueryNodes().isEmpty()) {
            project(preComputer.getSubQueryNodes(), preComputer.getSubQueryAliases(), true);
            nodeList =
                    newNodeList.stream()
                            .map(k -> k.accept(new RexTmpVariableConverter(true, this)))
                            .collect(Collectors.toList());
            input = requireNonNull(peek(), "frame stack is empty");
        }

        PREPARE_PROJECT_ARGS:
        {
            // if project denotes the `select('a')` in gremlin, give a default alias to skip the
            // real projection
            if (projectOneTag(nodeList, fieldNameList, isAppend) != null) {
                fieldNameList = ImmutableList.of(AliasInference.DEFAULT_NAME);
                break PREPARE_PROJECT_ARGS;
            } else if (input instanceof Project) {
                // fuse the project with the previous node if meets the following requirements :
                // 1. the input is project
                // 2. the expressions in the current project all start from the tags in the input
                // 3. the input denotes the `select('a')` in gremlin
                AliasNameWithId inputOneTag =
                        projectOneTag(
                                ((Project) input).getProjects(),
                                input.getRowType().getFieldNames(),
                                ((GraphLogicalProject) input).isAppend());
                if (inputOneTag != null) {
                    AliasNameWithId defaultAlias =
                            new AliasNameWithId(
                                    AliasInference.DEFAULT_NAME, AliasInference.DEFAULT_ID);
                    List<AliasNameWithId> inputTags = Lists.newArrayList(inputOneTag, defaultAlias);
                    if (projectPropertyOfTags(nodeList, inputTags)) {
                        inputTags.removeAll(Lists.newArrayList(defaultAlias));
                        if (inputTags.size() == 1) {
                            RexVariableAliasConverter converter =
                                    new RexVariableAliasConverter(
                                            true,
                                            this,
                                            inputTags.get(0).getAliasName(),
                                            inputTags.get(0).getAliasId());
                            nodeList =
                                    nodeList.stream()
                                            .map(k -> k.accept(converter))
                                            .collect(Collectors.toList());
                        }
                        // remove the input project
                        input = input.getInput(0);
                    }
                }
            }
            fieldNameList =
                    AliasInference.inferProject(
                            nodeList,
                            fieldNameList,
                            AliasInference.getUniqueAliasList(input, isAppend));
        }

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
     * check if the {@code exprs} are actually the pattern of `select('a')` in gremlin, return the tag if it is.
     * @param exprs
     * @param aliases
     * @return
     */
    private @Nullable AliasNameWithId projectOneTag(
            List<RexNode> exprs, List<String> aliases, boolean isAppend) {
        if (isAppend
                && exprs.size() == 1
                && exprs.get(0) instanceof RexGraphVariable
                && ((RexGraphVariable) exprs.get(0)).getProperty() == null
                && (aliases.isEmpty() || AliasInference.isDefaultAlias(aliases.get(0)))) {
            RexVariableAliasCollector<AliasNameWithId> collector =
                    new RexVariableAliasCollector<>(
                            true,
                            (RexGraphVariable var) -> {
                                String[] splits =
                                        var.getName()
                                                .split(Pattern.quote(AliasInference.DELIMITER));
                                String aliasName =
                                        splits.length > 0 ? splits[0] : AliasInference.DEFAULT_NAME;
                                return new AliasNameWithId(aliasName, var.getAliasId());
                            });
            return exprs.get(0).accept(collector).get(0);
        }
        return null;
    }

    /**
     * check if the {@code exprs} denotes the properties projection of the specified {@code tags}, return true if it is.
     * @param exprs
     * @param tags
     * @return
     */
    private boolean projectPropertyOfTags(List<RexNode> exprs, List<AliasNameWithId> tags) {
        List<Integer> tagIds = tags.stream().map(k -> k.getAliasId()).collect(Collectors.toList());
        RexVariableAliasCollector<Integer> collector =
                new RexVariableAliasCollector<>(true, (RexGraphVariable var) -> var.getAliasId());
        return exprs.stream().allMatch(k -> tagIds.containsAll(k.accept(collector)));
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
        List<RelDataTypeField> fields = Lists.newArrayList();
        for (int i = 0; i < aliasList.size(); ++i) {
            String aliasName = aliasList.get(i);
            fields.add(
                    new RelDataTypeFieldImpl(
                            aliasName, generateAliasId(aliasName), nodeList.get(i).getType()));
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
        return new GraphGroupKeys((List<RexNode>) flatExprs(variables), aliases);
    }

    // build aggregate functions

    /**
     * @param distinct
     * @param alias
     * @param operands keys to aggregate on, complex expressions (i.e. "a.age + 1") should be projected in advance
     * @return
     */
    public AggCall collect(boolean distinct, @Nullable String alias, RexNode... operands) {
        return aggregateCall(
                GraphStdOperatorTable.COLLECT,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    public AggCall collect(
            boolean distinct, @Nullable String alias, Iterable<? extends RexNode> operands) {
        return aggregateCall(
                GraphStdOperatorTable.COLLECT,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    public AggCall collect(RexNode... operands) {
        return collect(false, null, operands);
    }

    public AggCall collect(Iterable<? extends RexNode> operands) {
        return collect(false, null, operands);
    }

    /**
     *  {@code sum0} is an aggregator which returns the sum of the values which
     *  go into it like {@code sum}. It differs in that return zero for the null values instead of null.
     */
    public AggCall sum0(RexNode operand) {
        return this.sum(false, null, operand);
    }

    public AggCall sum0(boolean distinct, @Nullable String alias, RexNode operand) {
        return aggregateCall(
                GraphStdOperatorTable.SUM0,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of(operand));
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
        if (operands.isEmpty()) { // to support count star
            operands = ImmutableList.of(variable(AliasInference.STAR));
        }
        operands = ImmutableList.copyOf(flatExprs(operands));
        return new GraphAggCall(getCluster(), aggFunction, operands).as(alias).distinct(distinct);
    }

    @Override
    public GraphBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
        Objects.requireNonNull(groupKey);
        Objects.requireNonNull(aggCalls);

        RelNode input = requireNonNull(peek(), "frame stack is empty");

        Registrar registrar = new Registrar(this, input, true);
        List<RexNode> registerKeys =
                registrar.registerExpressions(((GraphGroupKeys) groupKey).getVariables());

        List<List<RexNode>> registerCallsList = new ArrayList<>();
        for (AggCall call : aggCalls) {
            registerCallsList.add(
                    registrar.registerExpressions(((GraphAggCall) call).getOperands()));
        }

        List<GraphAggCall> aggCallList = new ArrayList<>();
        // need to project in advance
        if (!registrar.getExtraNodes().isEmpty()) {
            project(registrar.getExtraNodes(), registrar.getExtraAliases(), registrar.isAppend());
            RexTmpVariableConverter converter = new RexTmpVariableConverter(true, this);
            groupKey =
                    new GraphGroupKeys(
                            registerKeys.stream()
                                    .map(k -> k.accept(converter))
                                    .collect(Collectors.toList()),
                            ((GraphGroupKeys) groupKey).getAliases());
            int i = 0;
            for (AggCall call : aggCalls) {
                GraphAggCall call1 = (GraphAggCall) call;
                aggCallList.add(
                        new GraphAggCall(
                                        call1.getCluster(),
                                        call1.getAggFunction(),
                                        registerCallsList.get(i).stream()
                                                .map(k -> k.accept(converter))
                                                .collect(Collectors.toList()))
                                .as(call1.getAlias())
                                .distinct(call1.isDistinct()));
                ++i;
            }
            input = requireNonNull(peek(), "frame stack is empty");
        } else {
            for (AggCall aggCall : aggCalls) {
                aggCallList.add((GraphAggCall) aggCall);
            }
        }
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
     *
     * @param offsetNode
     * @param fetchNode
     * @param nodes build limit() if empty
     * @return
     */
    @Override
    public GraphBuilder sortLimit(
            @Nullable RexNode offsetNode,
            @Nullable RexNode fetchNode,
            Iterable<? extends RexNode> nodes) {
        if (offsetNode != null && !(offsetNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("OFFSET node must be RexLiteral");
        }
        if (offsetNode != null && !(offsetNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("FETCH node must be RexLiteral");
        }

        nodes = flatExprs((Iterable<RexNode>) nodes);

        RelNode input = requireNonNull(peek(), "frame stack is empty");

        List<RelDataTypeField> originalFields = input.getRowType().getFieldList();

        Registrar registrar = new Registrar(this, input, true);
        List<RexNode> registerNodes = registrar.registerExpressions(ImmutableList.copyOf(nodes));

        // expressions need to be projected in advance
        if (!registrar.getExtraNodes().isEmpty()) {
            if (input.getRowType().getFieldList().size() == 1) {
                RelDataTypeField field = input.getRowType().getFieldList().get(0);
                // give a non-default alias to the head, so that the tail project can preserve the
                // head field
                if (field.getName() == AliasInference.DEFAULT_NAME) {
                    Set<String> uniqueAliases = AliasInference.getUniqueAliasList(input, true);
                    uniqueAliases.addAll(registrar.getExtraAliases());
                    String nonDefault = AliasInference.inferAliasWithPrefix("$f", uniqueAliases);
                    // set the non default alias to the input
                    as(nonDefault);
                    originalFields =
                            Lists.newArrayList(
                                    new RelDataTypeFieldImpl(
                                            nonDefault,
                                            generateAliasId(nonDefault),
                                            field.getType()));
                }
            }
            project(registrar.getExtraNodes(), registrar.getExtraAliases(), registrar.isAppend());
            RexTmpVariableConverter converter = new RexTmpVariableConverter(true, this);
            registerNodes =
                    registerNodes.stream()
                            .map(k -> k.accept(converter))
                            .collect(Collectors.toList());
            input = requireNonNull(peek(), "frame stack is empty");
        }

        List<RelFieldCollation> fieldCollations = fieldCollations(registerNodes);
        Config config = Utils.getFieldValue(RelBuilder.class, this, "config");

        // limit 0 -> return empty value
        if ((fetchNode != null && RexLiteral.intValue(fetchNode) == 0) && config.simplifyLimit()) {
            return (GraphBuilder) empty();
        }

        // output all results without any order -> skip
        if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
            return this; // sort is trivial
        }
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
        // to remove the extra columns we have added
        if (!registrar.getExtraAliases().isEmpty()) {
            List<RexNode> originalExprs = new ArrayList<>();
            List<String> originalAliases = new ArrayList<>();
            for (RelDataTypeField field : originalFields) {
                originalExprs.add(variable(field.getName()));
                originalAliases.add(field.getName());
            }
            project(originalExprs, originalAliases, false);
        }
        return this;
    }

    public GraphBuilder dedupBy(Iterable<? extends RexNode> nodes) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");

        nodes = flatExprs((Iterable<RexNode>) nodes);

        List<RelDataTypeField> originalFields = input.getRowType().getFieldList();

        Registrar registrar = new Registrar(this, input, true);
        List<RexNode> registerNodes = registrar.registerExpressions(ImmutableList.copyOf(nodes));

        // expressions need to be projected in advance
        if (!registrar.getExtraNodes().isEmpty()) {
            if (input.getRowType().getFieldList().size() == 1) {
                RelDataTypeField field = input.getRowType().getFieldList().get(0);
                // give a non-default alias to the head, so that the tail project can preserve the
                // head field
                if (field.getName() == AliasInference.DEFAULT_NAME) {
                    Set<String> uniqueAliases = AliasInference.getUniqueAliasList(input, true);
                    uniqueAliases.addAll(registrar.getExtraAliases());
                    String nonDefault = AliasInference.inferAliasWithPrefix("$f", uniqueAliases);
                    // set the non default alias to the input
                    as(nonDefault);
                    originalFields =
                            Lists.newArrayList(
                                    new RelDataTypeFieldImpl(
                                            nonDefault,
                                            generateAliasId(nonDefault),
                                            field.getType()));
                }
            }
            project(registrar.getExtraNodes(), registrar.getExtraAliases(), registrar.isAppend());
            RexTmpVariableConverter converter = new RexTmpVariableConverter(true, this);
            registerNodes =
                    registerNodes.stream()
                            .map(k -> k.accept(converter))
                            .collect(Collectors.toList());
            input = requireNonNull(peek(), "frame stack is empty");
        }

        // if dedup by keys is empty, use 'HEAD' variable by default
        if (registerNodes.isEmpty()) {
            registerNodes.add(variable((String) null));
        }
        RelNode dedupBy =
                GraphLogicalDedupBy.create(
                        (GraphOptCluster) this.getCluster(), input, registerNodes);
        replaceTop(dedupBy);
        // to remove the extra columns we have added
        if (!registrar.getExtraAliases().isEmpty()) {
            List<RexNode> originalExprs = new ArrayList<>();
            List<String> originalAliases = new ArrayList<>();
            for (RelDataTypeField field : originalFields) {
                originalExprs.add(variable(field.getName()));
                originalAliases.add(field.getName());
            }
            project(originalExprs, originalAliases, false);
        }
        return this;
    }

    public GraphBuilder unfold(RexNode unfoldKey, @Nullable String aliasName) {
        RelNode input = requireNonNull(peek(), "frame stack is empty");
        RelDataType keyType = unfoldKey.getType();
        Preconditions.checkArgument(
                keyType.getComponentType() != null,
                "input type of 'unfold' should be set or array with single component type, but is"
                        + " [%s]",
                keyType);
        GraphLogicalUnfold unfold =
                new GraphLogicalUnfold((GraphOptCluster) getCluster(), input, unfoldKey, aliasName);
        replaceTop(unfold);
        return this;
    }

    @Override
    public RelBuilder join(
            JoinRelType joinType, RexNode condition, Set<CorrelationId> variablesSet) {
        Join join = (Join) super.join(joinType, condition, variablesSet).peek();
        Utils.setFieldValue(AbstractRelNode.class, join, "rowType", reorgAliasId(join));
        return this;
    }

    @Override
    public RelBuilder antiJoin(Iterable<? extends RexNode> conditions) {
        Join join = (Join) super.antiJoin(conditions).peek();
        Utils.setFieldValue(AbstractRelNode.class, join, "rowType", reorgAliasId(join));
        return this;
    }

    /**
     * in the official implementation of {@code join}, the aliasId in rowType actually represents the columnId, but we need to preserve the original aliasId before the {@code join}
     * @param join
     * @return
     */
    private RelDataType reorgAliasId(Join join) {
        RelDataType originalType = join.getRowType();
        RelDataType leftType = join.getLeft().getRowType();
        RelDataType rightType = join.getRight().getRowType();
        List<RelDataTypeField> newFields =
                originalType.getFieldList().stream()
                        .map(
                                k -> {
                                    if (k.getIndex() < leftType.getFieldCount()) {
                                        RelDataTypeField leftField =
                                                leftType.getFieldList().get(k.getIndex());
                                        return new RelDataTypeFieldImpl(
                                                k.getName(), leftField.getIndex(), k.getType());
                                    } else {
                                        RelDataTypeField rightField =
                                                rightType
                                                        .getFieldList()
                                                        .get(
                                                                k.getIndex()
                                                                        - leftType.getFieldCount());
                                        return new RelDataTypeFieldImpl(
                                                k.getName(), rightField.getIndex(), k.getType());
                                    }
                                })
                        .collect(Collectors.toList());
        return new RelRecordType(StructKind.FULLY_QUALIFIED, newFields);
    }

    @Override
    public RexLiteral literal(@Nullable Object value) {
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if (value == null) {
            final RelDataType type = getTypeFactory().createSqlType(SqlTypeName.NULL);
            return rexBuilder.makeNullLiteral(type);
        } else if (value instanceof Boolean) {
            return rexBuilder.makeLiteral((Boolean) value);
        } else if (value instanceof BigDecimal) {
            return rexBuilder.makeExactLiteral((BigDecimal) value);
        } else if (value instanceof Float || value instanceof Double) {
            return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) value).doubleValue()));
        } else if (value instanceof Long) { // convert long to BIGINT, i.e. 2l
            return rexBuilder.makeBigintLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof Number) {
            return rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof String) {
            return rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Enum) {
            return rexBuilder.makeLiteral(
                    value, getTypeFactory().createSqlType(SqlTypeName.SYMBOL));
        } else {
            throw new IllegalArgumentException(
                    "cannot convert " + value + " (" + value.getClass() + ") to a constant");
        }
    }

    /**
     * create a list of {@code RelFieldCollation} by order keys
     *
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
     *
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

    @Override
    public RexNode equals(RexNode operand0, RexNode operand1) {
        return this.call(GraphStdOperatorTable.EQUALS, operand0, operand1);
    }

    @Override
    public RexNode not(RexNode operand) {
        return this.call(GraphStdOperatorTable.NOT, operand);
    }

    /**
     * {@code FilterIntoJoinRule} will put a new project on top of the join, but it is not necessary and will lead to extra cost, here we override it and do nothing to skip the extra project
     * @param castRowType
     * @param rename
     * @return
     */
    @Override
    public GraphBuilder convert(RelDataType castRowType, boolean rename) {
        // do nothing
        return this;
    }

    /**
     * assign a new alias to the top node, we implement the function specifically to support the gremlin's `as` step
     * @param alias
     * @return
     */
    @Override
    public GraphBuilder as(String alias) {
        RelNode top = requireNonNull(peek(), "frame stack is empty");
        // skip intermediate operations which make no changes to the row type, i.e.
        // filter/limit/dedup...
        RelNode parent = null;
        while (!top.getInputs().isEmpty() && top.getInput(0).getRowType() == top.getRowType()) {
            parent = top;
            top = top.getInput(0);
        }
        if (top instanceof AbstractBindableTableScan
                || top instanceof GraphLogicalPathExpand
                || top instanceof GraphLogicalProject
                || top instanceof GraphLogicalAggregate) {
            RelDataType rowType = top.getRowType();
            // we can assign the alias only if the top node has only one field, otherwise we skip
            // the
            // operation
            if (rowType.getFieldList().size() != 1) {
                return this;
            }
            build();
            if (!top.getInputs().isEmpty()) {
                push(top.getInput(0));
            }
            if (top instanceof GraphLogicalSource) {
                GraphLogicalSource source = (GraphLogicalSource) top;
                source(
                        new SourceConfig(
                                source.getOpt(), getLabelConfig(source.getTableConfig()), alias));
                if (source.getUniqueKeyFilters() != null) {
                    filter(source.getUniqueKeyFilters());
                }
                if (ObjectUtils.isNotEmpty(source.getFilters())) {
                    filter(source.getFilters());
                }
            } else if (top instanceof GraphLogicalExpand) {
                GraphLogicalExpand expand = (GraphLogicalExpand) top;
                expand(
                        new ExpandConfig(
                                expand.getOpt(), getLabelConfig(expand.getTableConfig()), alias));
                if (ObjectUtils.isNotEmpty(expand.getFilters())) {
                    filter(expand.getFilters());
                }
            } else if (top instanceof GraphLogicalGetV) {
                GraphLogicalGetV getV = (GraphLogicalGetV) top;
                getV(new GetVConfig(getV.getOpt(), getLabelConfig(getV.getTableConfig()), alias));
                if (ObjectUtils.isNotEmpty(getV.getFilters())) {
                    filter(getV.getFilters());
                }
            } else if (top instanceof GraphLogicalPathExpand) {
                GraphLogicalPathExpand pxdExpand = (GraphLogicalPathExpand) top;
                GraphLogicalExpand expand = (GraphLogicalExpand) pxdExpand.getExpand();
                GraphLogicalGetV getV = (GraphLogicalGetV) pxdExpand.getGetV();
                PathExpandConfig.Builder pxdBuilder = PathExpandConfig.newBuilder(this);
                RexNode offset = pxdExpand.getOffset(), fetch = pxdExpand.getFetch();
                pxdBuilder
                        .expand(
                                new ExpandConfig(
                                        expand.getOpt(),
                                        getLabelConfig(expand.getTableConfig()),
                                        expand.getAliasName()))
                        .getV(
                                new GetVConfig(
                                        getV.getOpt(),
                                        getLabelConfig(getV.getTableConfig()),
                                        getV.getAliasName()))
                        .pathOpt(pxdExpand.getPathOpt())
                        .resultOpt(pxdExpand.getResultOpt())
                        .range(
                                offset == null
                                        ? 0
                                        : ((RexLiteral) offset).getValueAs(Integer.class),
                                fetch == null ? -1 : ((RexLiteral) fetch).getValueAs(Integer.class))
                        .startAlias(pxdExpand.getStartAlias().getAliasName())
                        .alias(alias);
                pathExpand(pxdBuilder.buildConfig());
            } else if (top instanceof GraphLogicalProject) {
                GraphLogicalProject project = (GraphLogicalProject) top;
                project(project.getProjects(), Lists.newArrayList(alias), project.isAppend());
            } else if (top instanceof GraphLogicalAggregate) {
                GraphLogicalAggregate aggregate = (GraphLogicalAggregate) top;
                // if group key is empty, we can assign the alias to the single aggregated value in
                // group
                if (aggregate.getGroupKey().groupKeyCount() == 0
                        && aggregate.getAggCalls().size() == 1) {
                    GraphAggCall aggCall = aggregate.getAggCalls().get(0);
                    aggregate(aggregate.getGroupKey(), ImmutableList.of(aggCall.as(alias)));
                }
            }
            if (parent != null && peek() != top) {
                parent.replaceInput(0, build());
                push(parent);
            }
        }
        return this;
    }

    private LabelConfig getLabelConfig(TableConfig tableConfig) {
        List<String> labels =
                tableConfig.getTables().stream()
                        .map(k -> k.getQualifiedName().get(0))
                        .collect(Collectors.toList());
        LabelConfig labelConfig = new LabelConfig(tableConfig.isAll());
        labels.forEach(k -> labelConfig.addLabel(k));
        return labelConfig;
    }

    // flat each expression in the given expression list, i.e. flat `RexGraphVariableList([a, b,
    // c])` to `List of [RexGraphVariable(a), RexGraphVariable(b), RexGraphVariable(c)]`
    private Iterable<RexNode> flatExprs(Iterable<RexNode> exprs) {
        List<RexNode> flatExprs = Lists.newArrayList();
        exprs.forEach(
                expr -> {
                    if (expr instanceof RexCall) {
                        List<RexNode> operands = ((RexCall) expr).getOperands();
                        if (operands.stream()
                                .anyMatch(operand -> operand instanceof RexGraphVariableList)) {
                            switch (expr.getKind()) {
                                case DESCENDING:
                                    operands =
                                            (List<RexNode>)
                                                    flatExprs(ImmutableList.of(operands.get(0)));
                                    operands.forEach(operand -> flatExprs.add(desc(operand)));
                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                            "cannot flat operands of "
                                                    + expr.getKind()
                                                    + " operator");
                            }
                        } else {
                            flatExprs.add(expr);
                        }
                    } else if (expr instanceof RexGraphVariableList) {
                        flatExprs.addAll((RexGraphVariableList) expr);
                    } else {
                        flatExprs.add(expr);
                    }
                });
        return flatExprs;
    }
}
