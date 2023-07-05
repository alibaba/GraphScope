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

package com.alibaba.graphscope.common.ir.procedure;

import com.alibaba.graphscope.common.ir.procedure.reader.StoredProceduresReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphStoredProcedures implements StoredProcedures {
    private final RelDataTypeFactory typeFactory;
    private final Map<String, StoredProcedureMeta> storedProcedureMetaMap;

    public GraphStoredProcedures(StoredProceduresReader reader) throws IOException {
        this.typeFactory = new JavaTypeFactoryImpl();
        this.storedProcedureMetaMap = Maps.newLinkedHashMap();
        for (URI uri : reader.getAllProcedureUris()) {
            StoredProcedureMeta createdMeta =
                    createStoredProcedureMeta(reader.getProcedureMeta(uri));
            this.storedProcedureMetaMap.put(createdMeta.getName(), createdMeta);
        }
    }

    @Override
    public @Nullable StoredProcedureMeta getStoredProcedure(String procedureName) {
        return this.storedProcedureMetaMap.get(procedureName);
    }

    private StoredProcedureMeta createStoredProcedureMeta(String procedureMeta) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(procedureMeta);
        String procedureName = (String) config.get("name");
        return new StoredProcedureMeta(
                procedureName,
                createReturnType((List) config.get("returns")),
                createParameters((List) config.get("params")));
    }

    private RelDataType createReturnType(List config) {
        List<RelDataTypeField> fields = Lists.newArrayList();
        Iterator iterator = config.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Map<String, Object> field = (Map<String, Object>) iterator.next();
            fields.add(
                    new RelDataTypeFieldImpl(
                            (String) field.get("name"),
                            index,
                            createDataType((String) field.get("type"))));
            ++index;
        }
        return new RelRecordType(fields);
    }

    private List<StoredProcedureMeta.Parameter> createParameters(List config) {
        List<StoredProcedureMeta.Parameter> parameters = Lists.newArrayList();
        Iterator iterator = config.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> parameter = (Map<String, Object>) iterator.next();
            parameters.add(
                    new StoredProcedureMeta.Parameter(
                            (String) parameter.get("name"),
                            createDataType((String) parameter.get("type"))));
        }
        return parameters;
    }

    private RelDataType createDataType(String typeString) {
        typeString = typeString.toUpperCase().replaceAll("\\s*", "");
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
            case "MULTISET(STRING)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.CHAR), -1);
            case "MULTISET(INTEGER)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
            case "MULTISET(BOOLEAN)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);
            case "MULTISET(FLOAT)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.FLOAT), -1);
            case "MULTISET(DOUBLE)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
            case "MULTISET(LONG)":
                return typeFactory.createMultisetType(
                        typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
            case "ARRAY(STRING)":
                return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.CHAR), -1);
            case "ARRAY(INTEGER)":
                return typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
            case "ARRAY(BOOLEAN)":
                return typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);
            case "ARRAY(FLOAT)":
                return typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.FLOAT), -1);
            case "ARRAY(DOUBLE)":
                return typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
            case "ARRAY(LONG)":
                return typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
            default:
                throw new UnsupportedOperationException("unsupported type: " + typeString);
        }
    }
}
