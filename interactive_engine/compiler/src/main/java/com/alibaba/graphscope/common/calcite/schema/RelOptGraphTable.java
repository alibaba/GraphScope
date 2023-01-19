package com.alibaba.graphscope.common.calcite.schema;

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.calcite.rel.builder.config.ScanOpt;
import com.alibaba.graphscope.common.calcite.schema.type.GraphSchemaType;
import com.alibaba.graphscope.common.calcite.schema.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.calcite.schema.type.LabelType;
import com.alibaba.maxgraph.compiler.api.schema.*;

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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * maintain {@link RelDataType} and {@link Statistic} per entity or per relation
 */
public class RelOptGraphTable implements RelOptTable {
    private List<String> tableName;
    private RelOptSchema schema;
    private RelDataType dataType;
    private Statistic statistic;

    protected RelOptGraphTable(
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
            return new GraphSchemaType(ScanOpt.Entity, labelType, fields);
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
                fuzzyTypes.add(new GraphSchemaType(ScanOpt.Relation, labelType, fields));
            }
            ObjectUtils.requireNonEmpty(fuzzyTypes);
            if (fuzzyTypes.size() == 1) {
                return fuzzyTypes.get(0);
            } else {
                return new GraphSchemaTypeList(ScanOpt.Relation, fuzzyTypes);
            }
        } else {
            throw new IllegalArgumentException("");
        }
    }

    private RelDataType deriveType(GraphProperty property) {
        RelDataTypeFactory typeFactory = this.schema.getTypeFactory();
        requireNonNull(typeFactory, "typeFactory");
        switch (property.getDataType()) {
            case INT:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case BOOL:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            default:
                throw new NotImplementedException("");
        }
    }

    @Override
    public List<String> getQualifiedName() {
        return null;
    }

    @Override
    public RelDataType getRowType() {
        return this.dataType;
    }

    /**
     * create {@link RelNode} of graph operators
     * @param toRelContext
     * @return
     */
    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        return null;
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return null;
    }

    // statistics
    @Override
    public double getRowCount() {
        return 0;
    }

    @Override
    public boolean isKey(ImmutableBitSet immutableBitSet) {
        return this.statistic.isKey(immutableBitSet);
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return this.statistic.getKeys();
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return null;
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        throw new NotImplementedException("");
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        throw new NotImplementedException("");
    }

    // not used currently
    @Override
    public @Nullable Expression getExpression(Class aClass) {
        throw new NotImplementedException("");
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> list) {
        throw new NotImplementedException("");
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        throw new NotImplementedException("");
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        throw new NotImplementedException("");
    }
}
