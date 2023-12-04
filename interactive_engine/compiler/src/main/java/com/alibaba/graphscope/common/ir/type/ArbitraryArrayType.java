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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * introduce a new array type to allow different component types in a single array,
 * to support {@code ListLiteral} in cypher, i.e. [a.name, a, b.age]
 */
public class ArbitraryArrayType extends AbstractSqlType {
    private final List<RelDataType> componentTypes;

    public ArbitraryArrayType(List<RelDataType> componentTypes, boolean isNullable) {
        super(SqlTypeName.ARRAY, isNullable, null);
        this.componentTypes = Objects.requireNonNull(componentTypes);
        this.computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("(" + this.componentTypes.toString() + ") ARRAY");
    }

    public List<RelDataType> getComponentTypes() {
        return Collections.unmodifiableList(componentTypes);
    }
}
