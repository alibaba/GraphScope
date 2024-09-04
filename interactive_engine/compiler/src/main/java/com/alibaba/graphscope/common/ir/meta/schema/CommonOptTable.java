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

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.common.ir.tools.Utils;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * to support common partial computation, we need to wrap the common sub-plan into a {@code RelOptTable}, i.e.
 * in the query `g.V().out().union(out(), out())`, `g.V().out()` is a common sub-plan, which is denoted as a {@code CommonOptTable} here.
 */
public class CommonOptTable implements RelOptTable {
    private final RelNode common;

    public CommonOptTable(RelNode common) {
        this.common = common;
    }

    @Override
    public List<String> getQualifiedName() {
        return ImmutableList.of("common#" + this.common.explain().hashCode());
    }

    public RelNode getCommon() {
        return common;
    }

    @Override
    public RelDataType getRowType() {
        return Utils.getOutputType(this.common);
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return null;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet immutableBitSet) {
        throw new UnsupportedOperationException("is key is unsupported yet in statistics");
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        throw new UnsupportedOperationException("get keys is unsupported yet in statistics");
    }

    @Override
    public double getRowCount() {
        RelMetadataQuery mq = common.getCluster().getMetadataQuery();
        return mq.getRowCount(common);
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        throw new UnsupportedOperationException("distribution is unsupported yet in statistics");
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        throw new UnsupportedOperationException("collations is unsupported yet in statistics");
    }

    // not used currently

    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        throw new UnsupportedOperationException("toRel is unsupported for it will never be used");
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        throw new UnsupportedOperationException(
                "referentialConstraints is unsupported for it will never be used");
    }

    @Override
    public @Nullable Expression getExpression(Class aClass) {
        throw new UnsupportedOperationException(
                "expression is unsupported for it will never be used");
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> list) {
        throw new UnsupportedOperationException("extend is unsupported for it will never be used");
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        throw new UnsupportedOperationException(
                "columnStrategies is unsupported for it will never be used");
    }
}
