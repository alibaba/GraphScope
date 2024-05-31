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

import java.util.Optional;

public interface GraphStatistics {

    /**
     * Gets the Cardinality of all nodes regardless of types.
     */
    Long getVertexCount();

    /**
     * Gets the Cardinality of all relationships regardless of types.
     */
    Long getEdgeCount();

    /**
     * Get the cardinality for the given vertex typeId
     *
     * @param vertexTypeId the vertex type id
     * @return the cardinality of the given vertex type id
     */
    Long getVertexTypeCount(Integer vertexTypeId);

    /*
     * Get the cardinality of all relationships (a)-[r]->(b), where
     *
     *   a has the type `sourceTypeId`, or any types if `sourceTypeId` is None
     *   b has the type `targetTypeId`, or any types if `targetTypeId` is None
     *   r has the type `edgeTypeId`, or any type if `edgeTypeId` is None
     *
     * @param sourceTypeId the source vertex type id, edgeTypeId the edge type id, targetTypeId the target vertex type id
     * @return the cardinality of the given relationship
     */
    Long getEdgeTypeCount(
            Optional<Integer> sourceTypeId,
            Optional<Integer> edgeTypeId,
            Optional<Integer> targetTypeId);

    /**
     * Get the version of the statistics, which should be consist with the version of schema
     *
     * @return The version of the statistics
     */
    String getVersion();
}
