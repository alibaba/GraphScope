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

package com.alibaba.graphscope.common.ir.type;

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.*;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Denote DataType of an entity or a relation, including opt, label and attributes
 */
public class GraphSchemaType extends RelRecordType {
    private final GraphOpt.Source scanOpt;
    private final GraphLabelType labelType;
    private final List<GraphSchemaType> fuzzySchemaTypes;

    /**
     * @param scanOpt   entity or relation
     * @param labelType
     * @param fields    attribute fields, each field denoted by {@link RelDataTypeField} which consist of property name, property id and type
     */
    public GraphSchemaType(
            GraphOpt.Source scanOpt, GraphLabelType labelType, List<RelDataTypeField> fields) {
        this(scanOpt, labelType, fields, false);
    }

    /**
     * add a constructor to accept {@code isNullable}, a nullable GraphSchemaType will be created after left outer join
     * @param scanOpt
     * @param labelType
     * @param fields
     * @param isNullable
     */
    public GraphSchemaType(
            GraphOpt.Source scanOpt,
            GraphLabelType labelType,
            List<RelDataTypeField> fields,
            boolean isNullable) {
        super(StructKind.NONE, fields, isNullable);
        this.scanOpt = scanOpt;
        Preconditions.checkArgument(
                labelType.getLabelsEntry().size() == 1,
                "can not use label=%s to init GraphSchemaType with single label",
                labelType);
        this.labelType = labelType;
        this.fuzzySchemaTypes = ImmutableList.of();
    }

    protected GraphSchemaType(
            GraphOpt.Source scanOpt,
            GraphLabelType labelType,
            List<RelDataTypeField> fields,
            List<GraphSchemaType> fuzzySchemaTypes,
            boolean isNullable) {
        super(StructKind.NONE, fields, isNullable);
        this.scanOpt = scanOpt;
        this.fuzzySchemaTypes = Objects.requireNonNull(fuzzySchemaTypes);
        this.labelType = labelType;
    }

    public static GraphSchemaType create(
            List<GraphSchemaType> list, RelDataTypeFactory typeFactory) {
        return create(list, typeFactory, false);
    }

    public static GraphSchemaType create(
            List<GraphSchemaType> list, RelDataTypeFactory typeFactory, boolean isNullable) {
        ObjectUtils.requireNonEmpty(list, "schema type list should not be empty");
        if (list.size() == 1) {
            return list.get(0);
        }
        GraphOpt.Source scanOpt = list.get(0).getScanOpt();
        List<String> labelOpts = Lists.newArrayList();
        List<RelDataTypeField> fields = Lists.newArrayList();
        List<RelDataTypeField> commonFields = Lists.newArrayList(list.get(0).getFieldList());
        List<GraphLabelType.Entry> fuzzyEntries = Lists.newArrayList();
        for (GraphSchemaType type : list) {
            Preconditions.checkArgument(
                    !type.fuzzy(),
                    "fuzzy label types nested in list of "
                            + GraphSchemaType.class
                            + " is considered to be invalid here");
            labelOpts.add(
                    "{label="
                            + type.getLabelType().getLabelsString()
                            + ", opt="
                            + type.scanOpt
                            + "}");
            if (type.getScanOpt() != scanOpt) {
                throw new IllegalArgumentException(
                        "fuzzy label types should have the same opt, but is " + labelOpts);
            }
            fields.addAll(type.getFieldList());
            commonFields.retainAll(type.getFieldList());
            fuzzyEntries.addAll(type.getLabelType().getLabelsEntry());
        }
        fields =
                fields.stream()
                        .distinct()
                        .map(
                                k -> {
                                    if (!commonFields.contains(
                                            k)) { // can be optional for some labels
                                        return new RelDataTypeFieldImpl(
                                                k.getName(),
                                                k.getIndex(),
                                                typeFactory.createTypeWithNullability(
                                                        k.getType(), true));
                                    }
                                    return k;
                                })
                        .collect(Collectors.toList());
        return new GraphSchemaType(
                scanOpt, new GraphLabelType(fuzzyEntries), fields, list, isNullable);
    }

    public GraphOpt.Source getScanOpt() {
        return scanOpt;
    }

    public GraphLabelType getLabelType() {
        return labelType;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("Graph_Schema_Type");
        sb.append("(");
        sb.append("labels=" + labelType);
        sb.append(", properties=[");
        Iterator var3 =
                Ord.zip((List) Objects.requireNonNull(this.fieldList, "fieldList")).iterator();

        while (var3.hasNext()) {
            Ord<RelDataTypeField> ord = (Ord) var3.next();
            if (ord.i > 0) {
                sb.append(", ");
            }

            RelDataTypeField field = ord.e;
            if (withDetail) {
                sb.append(field.getType().getFullTypeString());
            } else {
                sb.append(field.getType().toString());
            }

            sb.append(" ");
            sb.append(field.getName());
        }
        sb.append("]");

        sb.append(")");
    }

    @Override
    protected void computeDigest() {
        StringBuilder sb = new StringBuilder();
        generateTypeString(sb, false);
        digest = sb.toString();
    }

    @Override
    public boolean isStruct() {
        return false;
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return scanOpt == GraphOpt.Source.VERTEX ? GraphTypeFamily.VERTEX : GraphTypeFamily.EDGE;
    }

    public List<GraphSchemaType> getSchemaTypeAsList() {
        return ObjectUtils.isEmpty(this.fuzzySchemaTypes)
                ? ImmutableList.of(this)
                : Collections.unmodifiableList(this.fuzzySchemaTypes);
    }

    public boolean fuzzy() {
        return this.labelType.getLabelsEntry().size() > 1 || this.fuzzySchemaTypes.size() > 1;
    }
}
