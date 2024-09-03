/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.*;

/**
 * Create/Update/Read/Delete vertex
 */
public interface VertexInterface {
    Result<String> addVertex(String graphId, VertexEdgeRequest request);

    Result<String> updateVertex(String graphId, VertexRequest request);

    Result<VertexData> getVertex(String graphId, String label, Object primaryKey);

    Result<String> deleteVertex(String graphId, String label, Object primaryKey);
}
