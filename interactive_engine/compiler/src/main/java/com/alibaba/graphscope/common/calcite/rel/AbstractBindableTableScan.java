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

package com.alibaba.graphscope.common.calcite.rel;

import com.alibaba.graphscope.common.calcite.type.GraphSchemaType;
import com.alibaba.graphscope.common.calcite.type.GraphSchemaTypeList;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A basic structure of graph operators
 */
public abstract class AbstractBindableTableScan extends TableScan {
    // for filter fusion
    protected ImmutableList<RexNode> filters;
    // for field trimmer
    protected ImmutableIntList project;

    protected TableConfig tableConfig;

    private int innerId;

    protected AbstractBindableTableScan(
            @Nullable RelOptCluster cluster, List<RelHint> hints, TableConfig tableConfig) {
        super(
                cluster,
                RelTraitSet.createEmpty(),
                hints,
                (tableConfig == null || ObjectUtils.isEmpty(tableConfig.getTables()))
                        ? null
                        : tableConfig.getTables().get(0));
        Objects.requireNonNull(tableConfig);
        ObjectUtils.requireNonEmpty(tableConfig.getTables());
        this.tableConfig = tableConfig;
        if (cluster != null && cluster instanceof GraphOptCluster) {
            this.innerId = ((GraphOptCluster) cluster).getNextRelNodeId();
        }
    }

    @Override
    public RelDataType deriveRowType() {
        List<GraphSchemaType> tableTypes = new ArrayList<>();
        List<RelOptTable> tables = this.tableConfig.getTables();
        for (RelOptTable table : tables) {
            GraphSchemaType type = (GraphSchemaType) table.getRowType();
            // flat fuzzy labels to the list
            if (type instanceof GraphSchemaTypeList) {
                tableTypes.addAll((GraphSchemaTypeList) type);
            } else {
                tableTypes.add(type);
            }
        }
        ObjectUtils.requireNonEmpty(tableTypes);
        GraphSchemaType graphType =
                (tableTypes.size() == 1)
                        ? tableTypes.get(0)
                        : GraphSchemaTypeList.create(tableTypes);
        RelRecordType rowType =
                new RelRecordType(
                        ImmutableList.of(
                                new RelDataTypeFieldImpl(getAliasName(), getAliasId(), graphType)));
        return rowType;
    }

    public String getAliasName() {
        Objects.requireNonNull(hints);
        if (hints.size() < 2) {
            throw new IllegalArgumentException(
                    "should have put alias config in the index 1 of the hints list");
        }
        RelHint aliasHint = hints.get(1);
        Objects.requireNonNull(aliasHint.kvOptions);
        String aliasName = aliasHint.kvOptions.get("name");
        Objects.requireNonNull(aliasName);
        return aliasName;
    }

    public int getAliasId() {
        Objects.requireNonNull(hints);
        if (hints.size() < 2) {
            throw new IllegalArgumentException(
                    "should have put alias config in the index 1 of the hints list");
        }
        RelHint aliasHint = hints.get(1);
        Objects.requireNonNull(aliasHint.kvOptions);
        String aliasId = aliasHint.kvOptions.get("id");
        Objects.requireNonNull(aliasId);
        return Integer.valueOf(aliasId);
    }

    // toString

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("tableConfig", tableConfig)
                .itemIf("fusedProject", project, !ObjectUtils.isEmpty(project))
                .itemIf("fusedFilter", filters, !ObjectUtils.isEmpty(filters));
    }

    @Override
    public String toString() {
        return "rel#" + this.innerId + ':' + getDigest();
    }

    public void setFilters(ImmutableList<RexNode> filters) {
        this.filters = filters;
    }
}
