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
package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphPropertyNotFoundException;

import java.util.List;
import java.util.Map;

public interface GraphSchema {
    /**
     * Get the graph element with given label
     *
     * @param label The given label
     * @return The graph element
     * @throws GraphElementNotFoundException exception if the label not exist
     */
    SchemaElement getSchemaElement(String label) throws GraphElementNotFoundException;

    /**
     * Get the graph element with given label id
     *
     * @param labelId The given label id
     * @return The graph element
     * @throws GraphElementNotFoundException exception if the label not exist
     */
    SchemaElement getSchemaElement(int labelId) throws GraphElementNotFoundException;

    /**
     * Get vertex list
     *
     * @return The graph vertex type list
     */
    List<VertexType> getVertexTypes();

    /**
     * Get edge list
     *
     * @return The graph edge type list
     */
    List<EdgeType> getEdgeTypes();

    /**
     * Get the property id with given property name
     *
     * @param propertyName The given property name
     * @return The <label id, property id> result
     * @throws GraphPropertyNotFoundException exception if property name not exist
     */
    Map<Integer, Integer> getPropertyId(String propertyName) throws GraphPropertyNotFoundException;

    /**
     * Get the property name with given property id
     *
     * @param propertyId The given property id
     * @return The <label name, property name> result
     * @throws GraphPropertyNotFoundException exception if property id not exist
     */
    Map<String, String> getPropertyName(int propertyId) throws GraphPropertyNotFoundException;

    /**
     * Get the property definition with given property name
     *
     * @param propertyName The given property name
     * @return The property definitions
     */
    Map<Integer, GraphProperty> getPropertyDefinitions(String propertyName);

    /**
     * Get the property definition with given label id and property id
     *
     * @param labelId    The given label id
     * @param propertyId The given property id
     * @return The property definition
     */
    GraphProperty getPropertyDefinition(int labelId, int propertyId);

    /**
     * Get the version of the schema
     *
     * @return The schema version
     */
    int getVersion();
}
