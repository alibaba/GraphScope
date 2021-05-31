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
package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;

import java.util.List;

public interface AddElementPropertyManager<M extends AddElementPropertyManager> {
    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType);

    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @param comment      The comment of the property
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType, String comment);

    /**
     * Add a property definition to the given element type
     *
     * @param propertyName The property name
     * @param dataType     The data type such as "int"/"long"/"string"/"float"/"double"
     * @param comment      The comment of the property
     * @param defaultValue The default value
     * @return The create type manager
     */
    M addProperty(String propertyName, String dataType, String comment, Object defaultValue);

    /**
     * Add the comment of this type
     *
     * @param comment The given comment
     * @return The create type manager
     */
    M comment(String comment);

    /**
     * Get element type name
     *
     * @return The label name
     */
    String getLabel();

    /**
     * Get property definitions
     *
     * @return The property definitions
     */
    List<GraphProperty> getPropertyDefinitions();
}
