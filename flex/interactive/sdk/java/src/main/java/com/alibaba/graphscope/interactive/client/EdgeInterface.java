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
import com.alibaba.graphscope.interactive.models.EdgeData;
import com.alibaba.graphscope.interactive.models.EdgeRequest;

/**
 * Interface for Create/Read/Update/Delete edge.
 */
public interface EdgeInterface {
    Result<EdgeData> getEdge(
            String graphName,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue);

    Result<String> addEdge(String graphName, EdgeRequest edgeRequest);

    Result<String> deleteEdge(
            String graphName,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue);

    Result<String> updateEdge(String graphName, EdgeRequest edgeRequest);
}
