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
package com.alibaba.graphscope.groot.common.schema.api;

import com.alibaba.graphscope.groot.common.exception.PropertyNotFoundException;
import com.alibaba.graphscope.groot.common.exception.TypeNotFoundException;

import java.util.List;
import java.util.Map;

public interface GraphSchema {
    GraphElement getElement(String label) throws TypeNotFoundException;

    GraphElement getElement(int labelId) throws TypeNotFoundException;

    List<GraphVertex> getVertexList();

    List<GraphEdge> getEdgeList();

    Integer getPropertyId(String propName) throws PropertyNotFoundException;

    String getPropertyName(int propId) throws PropertyNotFoundException;

    Map<GraphElement, GraphProperty> getPropertyList(String propName);

    Map<GraphElement, GraphProperty> getPropertyList(int propId);

    /**
     * Get the version of the schema
     *
     * @return The schema version
     */
    String getVersion();
}
