/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.common.ir.rel.LoadCSVTableScan;
import com.alibaba.graphscope.common.ir.rel.type.DataSource;
import com.alibaba.graphscope.common.ir.type.ExplicitRecordType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Define a specific table to denote the CSV source
 */
public class LoadCSVTable extends AbstractTable implements TranslatableTable {
    private final ImmutableList<String> name;
    private final DataSource source;
    // allow a nullable table type if the csv header is not provided
    private final @Nullable RelDataType rowType;

    public LoadCSVTable(DataSource source, @Nullable RelDataType rowType) {
        this.source = Objects.requireNonNull(source);
        this.name = source.getName();
        this.rowType = rowType;
    }

    /**
     * If the table type is null, returns a {@code ExplicitRecordType} which always returns the explicit type for each field
     * @param typeFactory
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return this.rowType == null
                ? new ExplicitRecordType(typeFactory.createSqlType(SqlTypeName.CHAR))
                : this.rowType;
    }

    public DataSource getSource() {
        return source;
    }

    public ImmutableList<String> getName() {
        return this.name;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable optTable) {
        return new LoadCSVTableScan(
                (GraphOptCluster) ctx.getCluster(), ctx.getTableHints(), this, null);
    }
}
