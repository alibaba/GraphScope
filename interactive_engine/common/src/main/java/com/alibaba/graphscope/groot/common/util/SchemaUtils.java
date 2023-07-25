/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.common.util;

import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaUtils {
    /**
     * Get primary key list for the given vertex type
     *
     * @param vertexType The given vertex type
     * @return The primary key id list
     */
    public static List<Integer> getVertexPrimaryKeyList(GraphVertex vertexType) {
        return vertexType.getPrimaryKeyList().stream()
                .map(GraphProperty::getId)
                .collect(Collectors.toList());
    }

    public static List<Integer> getEdgePrimaryKeyList(GraphEdge edgeType) {
        if (edgeType.getPrimaryKeyList() == null) {
            return null;
        }
        return edgeType.getPrimaryKeyList().stream()
                .map(GraphProperty::getId)
                .collect(Collectors.toList());
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
}
