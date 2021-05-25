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
 * The interface of create vertex type manager
 */
public interface CreateVertexTypeManager extends CreateElementTypeManager<CreateVertexTypeManager> {
    /**
     * Specify the primary key list for the vertex type
     *
     * @param primaryKeys The primary key list
     * @return The type manager
     */
    CreateVertexTypeManager primaryKey(String... primaryKeys);

    /**
     * Get primary key list for vertex type
     *
     * @return The primary key list
     */
    List<String> getPrimaryKeyList();
}
