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

package com.alibaba.graphscope.common.ir.schema;

import static com.alibaba.graphscope.common.ir.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.ir.type.LabelType;
import com.alibaba.graphscope.compiler.api.schema.*;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Maintain {@link RelDataType} and {@link Statistic} per entity or per relation
 */
public class GraphOptTable implements RelOptTable {
    private List<String> tableName;
    private RelOptSchema schema;
    private RelDataType dataType;
    private Statistic statistic;

    protected GraphOptTable(
            RelOptSchema schema,
            List<String> tableName,
            GraphElement element,
            Statistic statistic) {
        this.schema = schema;
        this.tableName = tableName;
        this.statistic = statistic;
        this.dataType = deriveType(element);
    }

    private RelDataType deriveType(GraphElement element) {
        List<GraphProperty> properties = element.getPropertyList();
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < properties.size(); ++i) {
            GraphProperty property = properties.get(i);
            fields.add(
                    new RelDataTypeFieldImpl(
                            property.getName(), property.getId(), deriveType(property)));
        }
        if (element instanceof GraphVertex) {
            LabelType labelType =
                    (new LabelType()).label(element.getLabel()).labelId(element.getLabelId());
            return new GraphSchemaType(GraphOpt.Source.VERTEX, labelType, fields);
        } else if (element instanceof GraphEdge) {
            GraphEdge edge = (GraphEdge) element;
            List<EdgeRelation> relations = edge.getRelationList();
            List<GraphSchemaType> fuzzyTypes = new ArrayList<>();
            for (EdgeRelation relation : relations) {
                LabelType labelType =
                        (new LabelType()).label(element.getLabel()).labelId(element.getLabelId());
                GraphVertex src = relation.getSource();
                GraphVertex dst = relation.getTarget();
                labelType.srcLabel(src.getLabel()).dstLabel(dst.getLabel());
                labelType.srcLabelId(src.getLabelId()).dstLabelId(dst.getLabelId());
                fuzzyTypes.add(new GraphSchemaType(GraphOpt.Source.EDGE, labelType, fields));
            }
            ObjectUtils.requireNonEmpty(fuzzyTypes);
            return (fuzzyTypes.size() == 1)
                    ? fuzzyTypes.get(0)
                    : GraphSchemaTypeList.create(fuzzyTypes);
        } else {
            throw new IllegalArgumentException("element should be vertex or edge");
        }
    }

    private RelDataType deriveType(GraphProperty property) {
        RelDataTypeFactory typeFactory = this.schema.getTypeFactory();
        requireNonNull(typeFactory, "typeFactory");
        switch (property.getDataType()) {
            case BOOL:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case STRING:
                return typeFactory.createSqlType(SqlTypeName.CHAR);
            case INT:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case LONG:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            default:
                throw new UnsupportedOperationException(
                        "type " + property.getDataType().name() + " not supported");
        }
    }

    @Override
    public List<String> getQualifiedName() {
        return this.tableName;
    }

    @Override
    public RelDataType getRowType() {
        return this.dataType;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return this.schema;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }
        return null;
    }

    // statistics

    @Override
    public boolean isKey(ImmutableBitSet immutableBitSet) {
        return this.statistic.isKey(immutableBitSet);
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return this.statistic.getKeys();
    }

    @Override
    public double getRowCount() {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }

    // not used currently

    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        throw RESOURCE.functionNotImplement(this.getClass()).ex();
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        throw RESOURCE.functionNotImplement(this.getClass()).ex();
    }

    @Override
    public @Nullable Expression getExpression(Class aClass) {
        throw RESOURCE.functionNotImplement(this.getClass()).ex();
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> list) {
        throw RESOURCE.functionNotImplement(this.getClass()).ex();
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        throw RESOURCE.functionNotImplement(this.getClass()).ex();
    }
}
