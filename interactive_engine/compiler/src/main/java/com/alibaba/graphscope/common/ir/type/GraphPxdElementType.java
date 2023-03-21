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

/**
 * each element type of path expand
 */
public class GraphPxdElementType extends AbstractSqlType {
    private final RelDataType expandType;
    private final RelDataType getVType;

    public GraphPxdElementType(RelDataType expandType, RelDataType getVType) {
        super(SqlTypeName.OTHER, false, null);
        this.expandType = expandType;
        this.getVType = getVType;
        computeDigest();
    }

    public RelDataType getExpandType() {
        return expandType;
    }

    public RelDataType getGetVType() {
        return getVType;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("GraphPxdElementType(");
        sb.append(this.expandType);
        sb.append(", ");
        sb.append(this.getVType);
        sb.append(")");
    }

    @Override
    public String getFullTypeString() {
        StringBuilder sb = new StringBuilder();
        generateTypeString(sb, true);
        return sb.toString();
    }
}
