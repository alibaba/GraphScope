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
package com.alibaba.maxgraph.api.manager;

import com.alibaba.maxgraph.structure.manager.record.AddEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.AddVertexManager;
import com.alibaba.maxgraph.structure.manager.record.DelEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.DelVertexManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateVertexManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface RecordProcessorManager {
    Vertex addVertex(AddVertexManager addVertexManager);

    void deleteVertex(DelVertexManager delVertexManager);

    Vertex updateVertex(UpdateVertexManager updateVertexManager);

    Edge addEdge(AddEdgeManager addEdgeManager);

    void deleteEdge(DelEdgeManager delEdgeManager);

    void updateEdge(UpdateEdgeManager updateEdgeManager);
}
