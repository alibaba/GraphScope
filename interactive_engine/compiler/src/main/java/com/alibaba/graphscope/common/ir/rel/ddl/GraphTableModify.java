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

package com.alibaba.graphscope.common.ir.rel.ddl;

import com.alibaba.graphscope.common.ir.rel.DataSourceOperation;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.type.ExplicitRecordType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Define modifications on a graph table, the modifications include: insert, update and delete
 */
public class GraphTableModify extends TableModify {
    protected GraphTableModify(
            RelOptCluster cluster,
            @Nullable RelOptTable targetGraph,
            RelNode source,
            Operation operation,
            List<String> updateColumns,
            List<RexNode> sourceExprs) {
        super(
                cluster,
                RelTraitSet.createEmpty(),
                targetGraph,
                null,
                source,
                operation,
                updateColumns,
                sourceExprs,
                false);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("input", getInput()).item("operation", getOperation());
    }

    public static class Insert extends GraphTableModify implements DataSourceOperation {
        private final TargetGraph target;
        private final String aliasName;
        private final int aliasId;

        public Insert(RelOptCluster cluster, RelNode source, TargetGraph target) {
            super(cluster, target.getOptTable(), source, Operation.INSERT, null, null);
            this.target = Objects.requireNonNull(target);
            this.aliasName =
                    AliasInference.inferDefault(
                            target.getAliasName(), AliasInference.getUniqueAliasList(source, true));
            this.aliasId = ((GraphOptCluster) cluster).getIdGenerator().generate(this.aliasName);
        }

        public String getAliasName() {
            return aliasName;
        }

        public int getAliasId() {
            return aliasId;
        }

        @Override
        public RelDataType deriveRowType() {
            return new RelRecordType(
                    ImmutableList.of(
                            new RelDataTypeFieldImpl(
                                    getAliasName(), getAliasId(), target.getSingleSchemaType())));
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("target", target).item("alias", getAliasName());
        }

        @Override
        public Insert copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Insert(getCluster(), sole(inputs), target);
        }

        @Override
        public RelNode accept(RelShuttle shuttle) {
            if (shuttle instanceof GraphShuttle) {
                return ((GraphShuttle) shuttle).visit(this);
            }
            return shuttle.visit(this);
        }

        @Override
        public RelNode getExternalSource() {
            RelNode externalSource = input;
            while (externalSource instanceof Insert) {
                externalSource = ((Insert) externalSource).getInput();
            }
            return externalSource;
        }

        @Override
        public TargetGraph getTargetGraph() {
            return this.target;
        }
    }

    public static class Update extends GraphTableModify {
        private final FieldMappings updateMappings;

        public Update(RelOptCluster cluster, RelNode input, FieldMappings updateMappings) {
            super(
                    cluster,
                    GraphTableModify.mockOptTable(cluster),
                    input,
                    Operation.UPDATE,
                    ImmutableList.of(),
                    ImmutableList.of());
            this.updateMappings = Objects.requireNonNull(updateMappings);
        }

        public FieldMappings getUpdateMappings() {
            return updateMappings;
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("updateMappings", updateMappings);
        }

        @Override
        public Update copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Update(getCluster(), sole(inputs), updateMappings);
        }

        @Override
        public RelNode accept(RelShuttle shuttle) {
            if (shuttle instanceof GraphShuttle) {
                return ((GraphShuttle) shuttle).visit(this);
            }
            return shuttle.visit(this);
        }
    }

    public static class Delete extends GraphTableModify {
        private final List<RexNode> deleteExprs;

        public Delete(RelOptCluster cluster, RelNode input, List<RexNode> deleteExprs) {
            super(
                    cluster,
                    GraphTableModify.mockOptTable(cluster),
                    input,
                    Operation.DELETE,
                    null,
                    null);
            this.deleteExprs = Objects.requireNonNull(deleteExprs);
        }

        public List<RexNode> getDeleteExprs() {
            return Collections.unmodifiableList(deleteExprs);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("deleteExprs", deleteExprs);
        }

        @Override
        public Delete copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Delete(getCluster(), sole(inputs), deleteExprs);
        }

        @Override
        public RelNode accept(RelShuttle shuttle) {
            if (shuttle instanceof GraphShuttle) {
                return ((GraphShuttle) shuttle).visit(this);
            }
            return shuttle.visit(this);
        }
    }

    private static RelOptTable mockOptTable(RelOptCluster cluster) {
        MockTable table = new MockTable();
        return RelOptTableImpl.create(
                null, table.getRowType(cluster.getTypeFactory()), table, ImmutableList.of());
    }

    private static class MockTable extends AbstractTable implements TranslatableTable {
        @Override
        public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
            return null;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return new ExplicitRecordType(typeFactory.createSqlType(SqlTypeName.ANY));
        }
    }
}
