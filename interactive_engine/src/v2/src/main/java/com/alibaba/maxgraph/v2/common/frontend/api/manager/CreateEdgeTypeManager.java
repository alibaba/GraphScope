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

import java.util.List;

/**
 * The interface of create edge type manager
 */
public interface CreateEdgeTypeManager extends CreateElementTypeManager<CreateEdgeTypeManager> {
    /**
     * Add relation sourceVertexLabel->edgeLabel->targetVertexLabel to given edge type
     *
     * @param sourceVertexLabel The given source vertex label
     * @param targetVertexLabel The given target vertex label
     * @return The type manager
     */
    CreateEdgeTypeManager addRelation(String sourceVertexLabel, String targetVertexLabel);

    /**
     * Get relation list from manager
     *
     * @return The result relation list
     */
    List<EdgeRelationEntity> getRelationList();
}
