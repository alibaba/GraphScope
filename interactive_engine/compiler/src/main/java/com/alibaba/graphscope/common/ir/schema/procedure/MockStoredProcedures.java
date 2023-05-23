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

package com.alibaba.graphscope.common.ir.schema.procedure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MockStoredProcedures implements StoredProcedures {
    private final RelDataTypeFactory typeFactory;
    private final Map<String, StoredProcedureMeta> storedProcedureMetaMap;

    public MockStoredProcedures(String json) throws IOException {
        this.typeFactory = new JavaTypeFactoryImpl();
        this.storedProcedureMetaMap = Maps.newLinkedHashMap();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        Preconditions.checkArgument(jsonNode.isArray());
        Iterator<JsonNode> iterator = jsonNode.iterator();
        while(iterator.hasNext()) {
            JsonNode procedure = iterator.next();
            String procedureName = procedure.get("name").asText();
            StoredProcedureMeta meta = new StoredProcedureMeta(
                    procedureName,
                    createReturnType(procedure.get("returnType")),
                    createParameters(procedure.get("parameters"))
            );
            this.storedProcedureMetaMap.put(procedureName, meta);
        }
    }

    @Override
    public @Nullable StoredProcedureMeta getStoredProcedure(String procedureName) {
        return this.storedProcedureMetaMap.get(procedureName);
    }

    private RelDataType createReturnType(JsonNode jsonNode) {
        Preconditions.checkArgument(jsonNode.isArray());
        List<RelDataTypeField> fields = Lists.newArrayList();
        Iterator<JsonNode> iterator = jsonNode.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            JsonNode field = iterator.next();
            fields.add(new RelDataTypeFieldImpl(field.get("name").asText(), index, createDataType(field.get("type").asText())));
            ++index;
        }
        return new RelRecordType(fields);
    }

    private List<StoredProcedureMeta.Parameter> createParameters(JsonNode jsonNode) {
        Preconditions.checkArgument(jsonNode.isArray());
        List<StoredProcedureMeta.Parameter> parameters = Lists.newArrayList();
        Iterator<JsonNode> iterator = jsonNode.iterator();
        while(iterator.hasNext()) {
            JsonNode parameter = iterator.next();
            parameters.add(new StoredProcedureMeta.Parameter(
                    parameter.get("name").asText(),
                    createDataType(parameter.get("type").asText())
            ));
        }
        return parameters;
    }

    private RelDataType createDataType(String typeString) {
        switch (typeString) {
            case "STRING":
                return typeFactory.createSqlType(SqlTypeName.CHAR);
            case "INTEGER":
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case "BOOLEAN":
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case "FLOAT":
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case "DOUBLE":
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case "LONG":
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            default:
                throw new UnsupportedOperationException("unsupported type: " + typeString);
        }
    }
}
