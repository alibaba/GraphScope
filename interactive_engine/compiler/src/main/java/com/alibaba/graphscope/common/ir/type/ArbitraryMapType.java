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
 * introduce a new map type to allow different keys or value types in a single map,
 * to support {@code MapLiteral} in cypher, i.e. [name: a.name, a: a, age: b.age]
 */
public class ArbitraryMapType extends AbstractSqlType {
    private final List<RelDataType> keyTypes;
    private final List<RelDataType> valueTypes;

    protected ArbitraryMapType(
            List<RelDataType> keyTypes, List<RelDataType> valueTypes, boolean isNullable) {
        super(SqlTypeName.MAP, isNullable, null);
        this.keyTypes = Objects.requireNonNull(keyTypes);
        this.valueTypes = Objects.requireNonNull(valueTypes);
        this.computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("(" + keyTypes.toString() + ", " + valueTypes.toString() + ") MAP");
    }

    public List<RelDataType> getKeyTypes() {
        return Collections.unmodifiableList(this.keyTypes);
    }

    public List<RelDataType> getValueTypes() {
        return Collections.unmodifiableList(this.valueTypes);
    }
}
