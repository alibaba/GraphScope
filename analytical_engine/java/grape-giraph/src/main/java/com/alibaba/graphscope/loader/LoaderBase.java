/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.loader;

import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Base interface defines behavior for a loader.
 */
public interface LoaderBase {

    void init(
            int workerId,
            int workerNum,
            int threadNum,
            FFIByteVecVector vidBuffers,
            FFIByteVecVector vertexDataBuffers,
            FFIByteVecVector edgeSrcIdBuffers,
            FFIByteVecVector edgeDstIdBuffers,
            FFIByteVecVector edgeDataBuffers,
            FFIIntVecVector vidOffsets,
            FFIIntVecVector vertexDataOffsets,
            FFIIntVecVector edgeSrcIdOffsets,
            FFIIntVecVector edgeDstIdOffsets,
            FFIIntVecVector edgeDataOffsets);

    /**
     * @param inputPath The path of input file.
     * @param vformatClass The class name of vertex format.
     * @return Return an integer contains type params info.
     */
    int loadVertices(String inputPath, String vformatClass)
            throws ExecutionException, InterruptedException, ClassNotFoundException, IOException;

    void loadEdges(String inputPath, String eformatClass) throws ExecutionException, InterruptedException, ClassNotFoundException, IOException;

    int concurrency();

}
