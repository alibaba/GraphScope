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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * collection type, i.e. type of path expand
 */
public class GraphArrayType extends AbstractSqlType {
    private final RelDataType elementType;

    public GraphArrayType(RelDataType elementType) {
        super(SqlTypeName.ARRAY, false, ImmutableList.of());
        this.elementType = requireNonNull(elementType, "elementType");
        computeDigest();
    }

    @Override
    public RelDataType getComponentType() {
        return elementType;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        if (withDetail) {
            sb.append(elementType.getFullTypeString());
        } else {
            sb.append(elementType.toString());
        }
        sb.append(" ARRAY");
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return this;
    }
}
