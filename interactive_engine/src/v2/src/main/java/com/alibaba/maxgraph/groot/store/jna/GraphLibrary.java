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
package com.alibaba.maxgraph.groot.store.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface GraphLibrary extends Library {
    GraphLibrary INSTANCE = Native.load("maxgraph_ffi", GraphLibrary.class);

    Pointer openGraphStore(byte[] config, int len);
    boolean closeGraphStore(Pointer pointer);
    JnaResponse writeBatch(Pointer pointer, long snapshotId, byte[] data, int len);
    JnaResponse getGraphDefBlob(Pointer pointer);
    JnaResponse ingestData(Pointer pointer, String dataPath);
    void dropJnaResponse(JnaResponse jnaResponse);

    Pointer createWrapperPartitionGraph(Pointer graphStore);
    void deleteWrapperPartitionGraph(Pointer wrapperPartitionGraph);
}
