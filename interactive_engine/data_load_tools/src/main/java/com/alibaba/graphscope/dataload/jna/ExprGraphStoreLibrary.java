/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.dataload.jna;

import com.alibaba.graphscope.dataload.jna.helper.IrFunctionMapper;
import com.alibaba.graphscope.dataload.jna.helper.IrTypeMapper;
import com.alibaba.graphscope.dataload.jna.type.*;
import com.google.common.collect.ImmutableMap;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface ExprGraphStoreLibrary extends Library {
    ExprGraphStoreLibrary INSTANCE =
            Native.load(
                    "graph_store",
                    ExprGraphStoreLibrary.class,
                    ImmutableMap.of(
                            Library.OPTION_TYPE_MAPPER, IrTypeMapper.INSTANCE,
                            Library.OPTION_FUNCTION_MAPPER, IrFunctionMapper.INSTANCE));

    Pointer initParserFromJson(String metaJson);

    void destroyParser(Pointer ptrParser);

    Pointer getVertexParser(Pointer ptrParser, String label);

    void destroyVertexParser(Pointer ptrParser);

    FfiVertexData.ByValue encodeVertex(Pointer ptrParser, String rowStr, char delim);

    Pointer getEdgeParser(Pointer ptrParser, FfiEdgeTypeTuple.ByValue label);

    void destroyEdgeParser(Pointer ptrParser);

    FfiEdgeData.ByValue encodeEdge(Pointer ptrParser, String rowStr, char delim);

    void destroyPropertyBytes(FfiPropertyBytes.ByValue bytes);

    Pointer initGraphLoader(String rootDir, long partitionId);

    Pointer initWriteVertex(long batchSize);

    boolean writeVertex(Pointer buffer, FfiVertexData.ByValue data);

    int finalizeWriteVertex(Pointer graphLoader, Pointer buffer);

    Pointer initWriteEdge(long batchSize);

    boolean writeEdge(Pointer buffer, FfiEdgeData.ByValue data);

    int finalizeWriteEdge(Pointer graphLoader, Pointer buffer, long curPartition, long partitions);

    ResultCode finalizeGraphLoading(Pointer graphLoader);

    long getVertexPartition(long vertexId, long partitions);

    String getSchemaJsonFromParser(Pointer parser);
}
