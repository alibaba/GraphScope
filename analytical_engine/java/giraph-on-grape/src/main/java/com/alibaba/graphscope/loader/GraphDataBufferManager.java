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

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

public interface GraphDataBufferManager {

    void addVertex(int threadId, Writable id, Writable value) throws IOException;

    void addEdges(int threadId, Writable id, Iterable<Edge> edges) throws IOException;

    void addEdge(int threadId, WritableComparable srcId, WritableComparable dstId, Writable value)
            throws IOException;

    void reserveNumVertices(int length);

    void reserveNumEdges(int length);

    void finishAdding();
}
