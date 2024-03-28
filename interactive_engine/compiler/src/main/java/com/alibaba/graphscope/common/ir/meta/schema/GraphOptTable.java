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

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.google.common.collect.Lists;

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
import java.util.Objects;

/**
 * Maintain {@link RelDataType} and {@link Statistic} per entity or per relation
 */
public class GraphOptTable implements RelOptTable {
    private List<String> tableName;
    private RelOptSchema schema;
    private RelDataType dataType;
    private final List<ImmutableBitSet> uniqueKeys;

    protected GraphOptTable(RelOptSchema schema, List<String> tableName, GraphElement element) {
        this.schema = schema;
        this.tableName = tableName;
        this.dataType = deriveType(element);
        this.uniqueKeys = getUniqueKeys(element, dataType, schema);
    }

    private RelDataType deriveType(GraphElement element) {
        List<GraphProperty> properties = element.getPropertyList();
        List<RelDataTypeField> fields = new ArrayList<>();
        boolean isColumnId =
                (this.schema instanceof GraphOptSchema)
                        ? ((GraphOptSchema) this.schema).getRootSchema().isColumnId()
                        : false;
        for (int i = 0; i < properties.size(); ++i) {
            GraphProperty property = properties.get(i);
            fields.add(
                    new RelDataTypeFieldImpl(
                            property.getName(),
                            isColumnId ? property.getId() : -1,
                            deriveType(property)));
        }
        if (element instanceof GraphVertex) {
            GraphLabelType labelType =
                    new GraphLabelType(
                            new GraphLabelType.Entry()
                                    .label(element.getLabel())
                                    .labelId(element.getLabelId()));
            return new GraphSchemaType(GraphOpt.Source.VERTEX, labelType, fields);
        } else if (element instanceof GraphEdge) {
            GraphEdge edge = (GraphEdge) element;
            List<EdgeRelation> relations = edge.getRelationList();
            List<GraphSchemaType> fuzzyTypes = new ArrayList<>();
            for (EdgeRelation relation : relations) {
                GraphLabelType.Entry labelEntry =
                        new GraphLabelType.Entry()
                                .label(element.getLabel())
                                .labelId(element.getLabelId());
                GraphVertex src = relation.getSource();
                GraphVertex dst = relation.getTarget();
                labelEntry.srcLabel(src.getLabel()).dstLabel(dst.getLabel());
                labelEntry.srcLabelId(src.getLabelId()).dstLabelId(dst.getLabelId());
                fuzzyTypes.add(
                        new GraphSchemaType(
                                GraphOpt.Source.EDGE, new GraphLabelType(labelEntry), fields));
            }
            ObjectUtils.requireNonEmpty(fuzzyTypes);
            return (fuzzyTypes.size() == 1)
                    ? fuzzyTypes.get(0)
                    : GraphSchemaType.create(fuzzyTypes, getRelOptSchema().getTypeFactory());
        } else {
            throw new IllegalArgumentException("element should be vertex or edge");
        }
    }

    private List<ImmutableBitSet> getUniqueKeys(
            GraphElement element, RelDataType schemaType, RelOptSchema schema) {
        boolean isColumnId =
                (schema instanceof GraphOptSchema)
                        ? ((GraphOptSchema) schema).getRootSchema().isColumnId()
                        : false;
        List<ImmutableBitSet> uniqueKeys = Lists.newArrayList();
        List<GraphProperty> primaryKeyList = element.getPrimaryKeyList();
        if (ObjectUtils.isNotEmpty(primaryKeyList)) {
            for (GraphProperty property : primaryKeyList) {
                for (int i = 0; i < schemaType.getFieldList().size(); ++i) {
                    RelDataTypeField field = schemaType.getFieldList().get(i);
                    if (field.getName().equals(property.getName())) {
                        // todo: support unique key consisting of multiple columns, here we assume
                        // there is only one column in a unique key
                        uniqueKeys.add(ImmutableBitSet.of(isColumnId ? field.getIndex() : i));
                        break;
                    }
                }
            }
        }
        return uniqueKeys;
    }

    private RelDataType deriveType(GraphProperty property) {
        RelDataTypeFactory typeFactory = this.schema.getTypeFactory();
        requireNonNull(typeFactory, "typeFactory");
        switch (property.getDataType()) {
            case BOOL:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case CHAR:
            case STRING:
                return typeFactory.createSqlType(SqlTypeName.CHAR);
            case SHORT:
            case INT:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case LONG:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case DATE:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case TIME32:
                return typeFactory.createSqlType(SqlTypeName.TIME);
            case TIMESTAMP:
                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
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
    public boolean isKey(ImmutableBitSet properties) {
        return this.uniqueKeys.contains(properties);
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return this.uniqueKeys;
    }

    @Override
    public double getRowCount() {
        throw new UnsupportedOperationException("row count is unsupported yet in statistics");
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphOptTable that = (GraphOptTable) o;
        return Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }
}
