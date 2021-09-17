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
package com.alibaba.maxgraph.compiler.api.schema;

import java.util.List;

/**
 * Element in graph schema, such as vertex/edge
 */
public interface GraphElement {
    /**
     * Get element label
     *
     * @return The element label
     */
    String getLabel();

    /**
     * Get element label id
     *
     * @return The element label id
     */
    int getLabelId();

    /**
     * Get element property list
     *
     * @return The property list
     */
    List<GraphProperty> getPropertyList();

    /**
     * Get property by property id
     *
     * @param propId The property id
     * @return The graph property
     */
    GraphProperty getProperty(int propId);

    /**
     * Get property by property name
     * @param propName The property name
     * @return The graph property
     */
    GraphProperty getProperty(String propName);

    /**
     * Get version of given graph element
     * @return
     */
    int getVersionId();

    /**
     * Get primary key list
     *
     * @return The primary key list
     */
    List<GraphProperty> getPrimaryKeyList();

    /**
     * Get indices of primary key properties
     *
     * @return Indices of pk properties
     */
    List<Integer> getPkPropertyIndices();

    /**
     * Get primary key constraint for the given vertex type
     *
     * @return The primary key constraint
     */
    PrimaryKeyConstraint getPrimaryKeyConstraint();
}
