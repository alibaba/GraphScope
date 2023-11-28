/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel.graph;

import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A basic structure of graph operators : Source / Expand / GetV
 */
public abstract class AbstractBindableTableScan extends TableScan {
    // for filter fusion
    protected @Nullable ImmutableList<RexNode> filters;
    // for field trimmer
    protected @Nullable ImmutableIntList project;

    protected @Nullable RelNode input;

    protected final TableConfig tableConfig;

    protected final String aliasName;

    protected final int aliasId;

    protected final AliasNameWithId startAlias;

    protected AbstractBindableTableScan(
            GraphOptCluster cluster,
            List<RelHint> hints,
            @Nullable RelNode input,
            TableConfig tableConfig,
            @Nullable String aliasName,
            AliasNameWithId startAlias) {
        super(
                cluster,
                RelTraitSet.createEmpty(),
                hints,
                (tableConfig == null || ObjectUtils.isEmpty(tableConfig.getTables()))
                        ? null
                        : tableConfig.getTables().get(0));
        this.input = input;
        this.tableConfig = Objects.requireNonNull(tableConfig);
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName);
        this.startAlias = Objects.requireNonNull(startAlias);
    }

    protected AbstractBindableTableScan(
            GraphOptCluster cluster,
            List<RelHint> hints,
            TableConfig tableConfig,
            String aliasName) {
        this(cluster, hints, null, tableConfig, aliasName, AliasNameWithId.DEFAULT);
    }

    @Override
    public RelDataType deriveRowType() {
        List<GraphSchemaType> tableTypes = new ArrayList<>();
        List<RelOptTable> tables = ObjectUtils.requireNonEmpty(this.tableConfig.getTables());
        RelDataTypeFactory typeFactory = tables.get(0).getRelOptSchema().getTypeFactory();
        for (RelOptTable table : tables) {
            GraphSchemaType type = (GraphSchemaType) table.getRowType();
            // flat fuzzy labels to the list
            tableTypes.addAll(type.getSchemaTypeAsList());
        }
        ObjectUtils.requireNonEmpty(tableTypes);
        GraphSchemaType graphType =
                (tableTypes.size() == 1)
                        ? tableTypes.get(0)
                        : GraphSchemaType.create(tableTypes, typeFactory);
        RelRecordType rowType =
                new RelRecordType(
                        ImmutableList.of(
                                new RelDataTypeFieldImpl(getAliasName(), getAliasId(), graphType)));
        return rowType;
    }

    public void setRowType(GraphSchemaType graphType) {
        rowType =
                new RelRecordType(
                        ImmutableList.of(
                                new RelDataTypeFieldImpl(getAliasName(), getAliasId(), graphType)));
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public int getAliasId() {
        return this.aliasId;
    }

    public TableConfig getTableConfig() {
        return this.tableConfig;
    }

    // toString

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.itemIf("input", input, !Objects.isNull(input))
                .item("tableConfig", tableConfig)
                .item("alias", AliasInference.SIMPLE_NAME(getAliasName()))
                .itemIf(
                        "startAlias",
                        startAlias.getAliasName(),
                        startAlias.getAliasName() != AliasInference.DEFAULT_NAME)
                .itemIf("fusedProject", project, !ObjectUtils.isEmpty(project))
                .itemIf("fusedFilter", filters, !ObjectUtils.isEmpty(filters));
    }

    @Override
    public List<RelNode> getInputs() {
        return this.input == null ? ImmutableList.of() : ImmutableList.of(this.input);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        if (this.input == null) return;
        assert ordinalInParent == 0;
        this.input = p;
        this.recomputeDigest();
    }

    public void setFilters(ImmutableList<RexNode> filters) {
        this.filters = Objects.requireNonNull(filters);
    }

    public @Nullable ImmutableList<RexNode> getFilters() {
        return filters;
    }

    public AliasNameWithId getStartAlias() {
        return startAlias;
    }
}
