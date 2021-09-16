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
package com.alibaba.maxgraph.v2.common.util;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.*;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
    public static List<Integer> getVertexPrimaryKeyList(VertexType vertexType) {
        return vertexType.getPrimaryKeyConstraint().getPrimaryKeyList().stream().map(v -> vertexType.getProperty(v).getId()).collect(Collectors.toList());
    }

    public static List<Integer> getEdgePrimaryKeyList(EdgeType edgeType) {
        PrimaryKeyConstraint primaryKeyConstraint = edgeType.getPrimaryKeyConstraint();
        if (primaryKeyConstraint == null) {
            return null;
        }
        return primaryKeyConstraint.getPrimaryKeyList().stream()
                .map(v -> edgeType.getProperty(v).getId()).collect(Collectors.toList());
    }

    /**
     * Check if the given property id is primary key in the given vertex type
     *
     * @param propId     The given property id
     * @param schema     The given schema
     * @param vertexType The given vertex type
     * @return The true means the property id is primary key
     */
    public static boolean isPropPrimaryKey(int propId, GraphSchema schema, VertexType vertexType) {
        String propName = getPropertyName(propId, schema);
        return vertexType.getPrimaryKeyConstraint().getPrimaryKeyList().contains(propName);
    }

    /**
     * Check if the given property name exist in the schema
     *
     * @param propName The given property name
     * @param schema   The given property
     * @return The true means the property name exist in the schema
     */
    public static boolean checkPropExist(String propName, GraphSchema schema) {
        try {
            Map<Integer, Integer> labelPropertyIds = schema.getPropertyId(propName);
            return (null != labelPropertyIds) && (!labelPropertyIds.isEmpty());
        } catch (Exception ignored) {
            return false;
        }
    }

    /**
     * Get property name with the given property id and schema
     *
     * @param propId The given property id
     * @param schema The given schema
     * @return The property name
     */
    public static String getPropertyName(int propId, GraphSchema schema) {
        Map<String, String> labelPropertyNames = schema.getPropertyName(propId);
        Set<String> propertyNames = Sets.newHashSet(labelPropertyNames.values());
        if (propertyNames.size() > 1) {
            throw new UnsupportedOperationException("there're multiple property name for property id " + propId + " in " + labelPropertyNames);
        }
        return propertyNames.iterator().next();
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

        for (GraphProperty propertyDef : schema.getPropertyDefinitions(propName).values()) {
            dataTypeList.add(propertyDef.getDataType());
        }

        return dataTypeList;
    }
}
