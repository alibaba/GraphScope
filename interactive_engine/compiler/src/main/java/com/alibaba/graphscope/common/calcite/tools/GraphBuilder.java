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
import static com.alibaba.graphscope.common.calcite.util.Static.SIMPLE_NAME;

import com.alibaba.graphscope.common.calcite.rel.*;
import com.alibaba.graphscope.common.calcite.rex.RexCallBinding;
import com.alibaba.graphscope.common.calcite.rex.RexGraphVariable;
import com.alibaba.graphscope.common.calcite.rex.RexVariableAliasChecker;
import com.alibaba.graphscope.common.calcite.tools.config.*;
import com.alibaba.graphscope.common.calcite.type.GraphSchemaType;
import com.alibaba.graphscope.common.calcite.util.Static;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
            @Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    /**
     * @param context
     * @param cluster
     * @param relOptSchema
     * @return
     */
    public static GraphBuilder create(
            @Nullable Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        return new GraphBuilder(context, cluster, relOptSchema);
    }

    /**
     * validate and build an algebra structure of {@link LogicalSource},
     * how to validate:
     * validate the existence of the given labels in config,
     * if exist then derive the {@link GraphSchemaType} of the given labels and keep the type in {@link RelNode#getRowType()}
     *
     * @param config
     * @return
     */
    public GraphBuilder source(SourceConfig config) {
        LabelConfig labelConfig = config.getLabels();
        if (!labelConfig.isAll()) {
            ObjectUtils.requireNonEmpty(labelConfig.getLabels());
            List<RelOptTable> tables = new ArrayList<>();
            for (String label : labelConfig.getLabels()) {
                tables.add(relOptSchema.getTableForMember(ImmutableList.of(label)));
            }
            RelNode source =
                    LogicalSource.create(
                            cluster,
                            getHints(config),
                            new TableConfig(tables).isAll(labelConfig.isAll()));
            push(source);
            return this;
        }
        throw new UnsupportedOperationException("Non specific labels in source not supported");
    }

    private List<RelHint> getHints(SourceConfig config) {
        Objects.requireNonNull(config.getLabels());
        Objects.requireNonNull(config.getOpt());
        Objects.requireNonNull(config.getAlias());
        RelHint optHint = RelHint.builder("opt").hintOption(config.getOpt().name()).build();
        RelHint aliasHint =
                RelHint.builder("alias")
                        .hintOption("name", config.getAlias())
                        .hintOption("id", String.valueOf(generateAliasId(config.getAlias())))
                        .build();
        return ImmutableList.of(optHint, aliasHint);
    }

    private List<RelHint> getHints(ExpandConfig config) {
        return null;
    }

    private List<RelHint> getHints(GetVConfig config) {
        return null;
    }

    /**
     * generate a new alias id for the given alias name
     *
     * @param alias
     * @return
     * @throws Exception - if the given alias exist
     */
    private int generateAliasId(@Nullable String alias) {
        if (alias == null || alias == Static.Alias.DEFAULT_NAME) return -1;
        return 0;
    }

    /**
     * validate and build an algebra structure of {@link LogicalExpand}
     *
     * @param config
     * @return
     */
    public GraphBuilder expand(ExpandConfig config) {
        return null;
    }

    /**
     * validate and build an algebra structure of {@link LogicalGetV}
     *
     * @param config
     * @return
     */
    public GraphBuilder getV(GetVConfig config) {
        return null;
    }

    /**
     * build an algebra structure of {@link LogicalPathExpand}
     *
     * @param expandV expand with getV
     * @param offset
     * @param fetch
     * @return
     */
    public GraphBuilder pathExpand(RelNode expandV, int offset, int fetch) {
        return null;
    }

    /**
     * validate and build an algebra structure of {@link LogicalMatch},
     * make sure there exists {@code num} sentences in current context.
     * how to validate:
     * check the graph pattern (lookup from the graph schema and check whether the links are all valid) denoted by each sentence one by one.
     *
     * @param sentences
     * @param opts
     * @return
     */
    public GraphBuilder match(Iterable<? extends RelNode> sentences, Iterable<MatchOpt> opts) {
        return null;
    }

    /**
     * validate and build {@link RexGraphVariable} from a given alias (i.e. "a")
     *
     * @param alias
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias) {
        alias = (alias == null) ? Static.Alias.DEFAULT_NAME : alias;
        RelDataTypeField aliasField = getAliasField(alias);
        return RexGraphVariable.of(aliasField.getIndex(), SIMPLE_NAME(alias), aliasField.getType());
    }

    /**
     * validate and build {@link RexGraphVariable} from a given variable containing fieldName (i.e. "a.name" or "name")
     *
     * @param alias
     * @param property
     * @return
     */
    public RexGraphVariable variable(@Nullable String alias, String property) {
        alias = (alias == null) ? Static.Alias.DEFAULT_NAME : alias;
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
                        String.format("%s.%s", SIMPLE_NAME(alias), property),
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

    private RelDataTypeField getAliasField(String alias) {
        Objects.requireNonNull(alias);
        int inputCount = size();
        List<String> aliases = new ArrayList<>();
        for (int inputOrdinal = inputCount - 1; inputOrdinal >= 0; --inputOrdinal) {
            RelNode cur = peek(inputCount, inputOrdinal);
            RelRecordType rowType = (RelRecordType) cur.getRowType();
            for (RelDataTypeField field : rowType.getFieldList()) {
                if (field.getName().equals(alias)) {
                    return field;
                }
                aliases.add(SIMPLE_NAME(field.getName()));
            }
            // should have found in the top node if alias name is default
            if (alias == Static.Alias.DEFAULT_NAME) break;
            // the history before this node have been removed
            if (removeHistory(cur)) break;
        }
        throw new IllegalArgumentException(
                "{alias="
                        + SIMPLE_NAME(alias)
                        + "} "
                        + "not found; expected aliases are: "
                        + aliases);
    }

    private boolean removeHistory(RelNode node) {
        return true;
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
                || sqlKind == SqlKind.OR;
    }

    @Override
    public GraphBuilder filter(RexNode... conditions) {
        ObjectUtils.requireNonEmpty(conditions);
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
        GraphBuilder builder = (GraphBuilder) super.filter(conditions);
        // fuse filter with the previous table scan if meets the conditions
        Filter filter;
        AbstractBindableTableScan tableScan;
        if ((filter = topFilter()) != null && (tableScan = inputTableScan(filter)) != null) {
            RexNode condition = filter.getCondition();
            RexVariableAliasChecker checker =
                    new RexVariableAliasChecker(true, tableScan.getAliasId());
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

    private Filter topFilter() {
        if (this.size() > 0 && this.peek() instanceof Filter) {
            return (Filter) this.peek();
        } else {
            return null;
        }
    }

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
    public GraphBuilder project(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> aliases,
            boolean isAppend) {
        return null;
    }

    // build group keys

    /**
     * @param nodes   group keys (variables), complex expressions (i.e. "a.age + 1") should be projected in advance
     * @param aliases
     * @return
     */
    public GroupKey groupKey(
            List<? extends RexNode> nodes, Iterable<? extends @Nullable String> aliases) {
        return null;
    }

    // global key, i.e. g.V().count()
    @Override
    public GroupKey groupKey() {
        return null;
    }

    // build aggregate functions

    /**
     * @param distinct
     * @param alias
     * @param operands keys (variables) to aggregate on, complex expressions (i.e. sum("a.age+1")) should be projected in advance
     * @return
     */
    public AggCall collect(boolean distinct, @Nullable String alias, RexNode... operands) {
        return null;
    }

    @Override
    public AggCall count(boolean distinct, @Nullable String alias, RexNode... operands) {
        return null;
    }

    @Override
    public AggCall sum(boolean distinct, @Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall avg(boolean distinct, @Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall min(@Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public AggCall max(@Nullable String alias, RexNode operand) {
        return null;
    }

    @Override
    public GraphBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
        return null;
    }

    @Override
    public RexNode desc(RexNode expr) {
        return null;
    }

    /**
     * @param nodes order keys (variables), complex expressions (i.e. "a.age + 1") should be projected in advance
     * @return
     */
    @Override
    public GraphBuilder sort(Iterable<? extends RexNode> nodes) {
        return sortLimit(-1, -1, nodes);
    }

    // to fuse order()+limit()
    @Override
    public GraphBuilder sortLimit(int offset, int fetch, Iterable<? extends RexNode> nodes) {
        return null;
    }

    @Override
    public RelBuilder limit(int offset, int fetch) {
        return null;
    }

    public void pop() {
        this.build();
    }
}
