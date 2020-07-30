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
package com.alibaba.maxgraph.compiler.utils;

import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.PropDataType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaUtils {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);
    private static final Set<String> tokenNameList = Sets.newHashSet(T.label.getAccessor(), T.id.getAccessor(), T.key.getAccessor(), T.value.getAccessor());

    public static List<Integer> getVertexPrimaryKeyList(GraphVertex vertexType) {
        return vertexType.getPrimaryKeyList().stream().map(GraphProperty::getId).collect(Collectors.toList());
    }

    public static boolean isPropPrimaryKey(int propId, GraphSchema schema, GraphVertex vertexType) {
        String propName = getPropertyName(propId, schema);
        return vertexType.getPrimaryKeyList().stream().map(GraphProperty::getName)
                .collect(Collectors.toList()).contains(propName);
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

    public static Set<PropDataType> getPropDataTypeList(String propName, GraphSchema schema) {
        Set<PropDataType> dataTypeList = Sets.newHashSet();
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
