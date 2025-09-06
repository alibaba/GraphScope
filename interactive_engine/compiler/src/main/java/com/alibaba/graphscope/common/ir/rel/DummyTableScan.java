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

package com.alibaba.graphscope.common.ir.rel;

import com.alibaba.graphscope.common.ir.rel.graph.AbstractBindableTableScan;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.type.ExplicitRecordType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class DummyTableScan extends AbstractBindableTableScan {
    public DummyTableScan(GraphOptCluster cluster, List<RelHint> hints) {
        super(cluster, hints, createTableConfig(cluster, new Table()), AliasInference.DEFAULT_NAME);
    }

    private static TableConfig createTableConfig(RelOptCluster cluster, Table table) {
        RelOptTable optTable =
                RelOptTableImpl.create(
                        null,
                        new ExplicitRecordType(
                                cluster.getTypeFactory().createSqlType(SqlTypeName.CHAR)),
                        table,
                        ImmutableList.of("dummy"));
        return new TableConfig(ImmutableList.of(optTable));
    }

    @Override
    public RelDataType deriveRowType() {
        return new RelRecordType(
                ImmutableList.of(
                        new RelDataTypeFieldImpl(
                                getAliasName(), getAliasId(), getTable().getRowType())));
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    private static class Table extends AbstractTable implements TranslatableTable {
        @Override
        public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable optTable) {
            return null;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return new ExplicitRecordType(typeFactory.createSqlType(SqlTypeName.CHAR));
        }
    }
}
