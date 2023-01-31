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

package com.alibaba.graphscope.common.calcite.type;

import com.alibaba.graphscope.common.calcite.tools.config.ScanOpt;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Denote DataType of an entity or a relation, including opt, label and attributes
 */
public class GraphSchemaType extends RelRecordType {
    protected ScanOpt scanOpt;
    protected LabelType labelType;

    protected GraphSchemaType(ScanOpt scanOpt, List<RelDataTypeField> fields) {
        this(scanOpt, LabelType.DEFAULT, fields);
    }

    /**
     * @param scanOpt   entity or relation
     * @param labelType
     * @param fields    attribute fields, each field denoted by {@link RelDataTypeField} which consist of property name, property id and type
     */
    public GraphSchemaType(ScanOpt scanOpt, LabelType labelType, List<RelDataTypeField> fields) {
        super(StructKind.NONE, fields, false);
        this.scanOpt = scanOpt;
        this.labelType = labelType;
    }

    public ScanOpt getScanOpt() {
        return scanOpt;
    }

    public LabelType getLabelType() {
        return labelType;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("Graph_Schema_Type");

        sb.append("(");
        Iterator var3 =
                Ord.zip((List) Objects.requireNonNull(this.fieldList, "fieldList")).iterator();

        while (var3.hasNext()) {
            Ord<RelDataTypeField> ord = (Ord) var3.next();
            if (ord.i > 0) {
                sb.append(", ");
            }

            RelDataTypeField field = (RelDataTypeField) ord.e;
            if (withDetail) {
                sb.append(field.getType().getFullTypeString());
            } else {
                sb.append(field.getType().toString());
            }

            sb.append(" ");
            sb.append(field.getName());
        }

        sb.append(")");
    }
}
