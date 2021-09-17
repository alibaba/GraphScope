/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.compiler.api.schema.*;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaUtils {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);
    private static final Set<String> tokenNameList = Sets.newHashSet(T.label.getAccessor(), T.id.getAccessor(), T.key.getAccessor(), T.value.getAccessor());

    /**
     * Get primary key list for the given vertex type
     *
     * @param vertexType The given vertex type
     * @return The primary key id list
     */
    public static List<Integer> getVertexPrimaryKeyList(GraphVertex vertexType) {
        return vertexType.getPrimaryKeyConstraint().getPrimaryKeyList().stream().map(v -> vertexType.getProperty(v).getId()).collect(Collectors.toList());
    }

    public static List<Integer> getEdgePrimaryKeyList(GraphEdge edgeType) {
        PrimaryKeyConstraint primaryKeyConstraint = edgeType.getPrimaryKeyConstraint();
        if (primaryKeyConstraint == null) {
            return null;
        }
        return primaryKeyConstraint.getPrimaryKeyList().stream()
                .map(v -> edgeType.getProperty(v).getId()).collect(Collectors.toList());
    }

    /**
     * Get data type list for given property name and schema
     *
     * @param propName The given property name
     * @param schema   The given schema
     * @return The given property data type list
     */
    public static Set<DataType> getDataTypeList(String propName, GraphSchema schema) {
        Set<DataType> dataTypeList = Sets.newHashSet();
        if (tokenNameList.contains(propName)) {
            return dataTypeList;
        }

        for (GraphProperty propertyDef : schema.getPropertyList(propName).values()) {
            dataTypeList.add(propertyDef.getDataType());
        }

        return dataTypeList;
    }

    public static boolean checkPropExist(String propName, GraphSchema schema) {
        try {
            schema.getPropertyId(propName);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static String getPropertyName(int propId, GraphSchema schema) {
        return schema.getPropertyName(propId);
    }

    public static int getPropId(String name, GraphSchema schema) {
        return schema.getPropertyId(name);
    }

    public static Set<DataType> getPropDataTypeList(String propName, GraphSchema schema) {
        Set<DataType> dataTypeList = Sets.newHashSet();
        if (tokenNameList.contains(propName)) {
            return dataTypeList;
        }

        try {
            for (GraphProperty propertyDef : schema.getPropertyList(propName).values()) {
                dataTypeList.add(propertyDef.getDataType());
            }
        } catch (Exception e) {
            logger.warn("parse data type for prop " + propName + " fail in schema " + schema, e);
        }

        return dataTypeList;
    }
}
