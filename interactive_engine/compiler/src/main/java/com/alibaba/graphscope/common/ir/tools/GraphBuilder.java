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

import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
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
     * validate and build an algebra structure of {@code GraphLogicalSource}.
     *
     * how to validate:
     * 1. validate the existence of the given labels in config,
     * if exist then derive the {@code GraphSchemaType} of the given labels and keep the type in {@link RelNode#getRowType()},
     * otherwise throw exceptions
     *
     * 2. validate the existence of the given alias in config, if exist throw duplication exceptions
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
     * validate and build an algebra structure of {@code GraphLogicalExpand}
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
     * validate and build an algebra structure of {@code GraphLogicalGetV}
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
     * build an algebra structure of {@code GraphLogicalPathExpand}
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
     * get all aliases stored by previous operators, to avoid duplicated alias creation
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
    public GraphBuilder match(RelNode single, MatchOpt opt) {
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
        return null;
    }

    /**
     * validate and build {@link RexGraphVariable} from a given variable containing fieldName (i.e. "a.name" or "name")
     *
     * @param alias
     * @param property
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias, String property) {
        return null;
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
        return null;
    }

    @Override
    public GraphBuilder filter(RexNode... conditions) {
        return filter(ImmutableList.copyOf(conditions));
    }

    @Override
    public GraphBuilder filter(Iterable<? extends RexNode> conditions) {
        return this;
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
        return this;
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
