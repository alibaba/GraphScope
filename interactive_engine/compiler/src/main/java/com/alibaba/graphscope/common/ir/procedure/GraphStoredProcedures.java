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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphStoredProcedures implements StoredProcedures {
    private static final Logger logger = LoggerFactory.getLogger(GraphStoredProcedures.class);
    private final RelDataTypeFactory typeFactory;
    private final Map<String, StoredProcedureMeta> storedProcedureMetaMap;

    public GraphStoredProcedures(String procedureDir) throws IOException {
        this.typeFactory = new JavaTypeFactoryImpl();
        this.storedProcedureMetaMap = Maps.newLinkedHashMap();
        File dir = new File(procedureDir);
        Preconditions.checkArgument(dir.exists() && dir.isDirectory());
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".yaml")) {
                StoredProcedureMeta meta = createStoredProcedureMeta(file);
                this.storedProcedureMetaMap.put(meta.getName(), meta);
            }
        }
    }

    @Override
    public @Nullable StoredProcedureMeta getStoredProcedure(String procedureName) {
        return this.storedProcedureMetaMap.get(procedureName);
    }

    private StoredProcedureMeta createStoredProcedureMeta(File yamlFile) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(new FileInputStream(yamlFile));
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
        typeString = typeString.toUpperCase();
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
