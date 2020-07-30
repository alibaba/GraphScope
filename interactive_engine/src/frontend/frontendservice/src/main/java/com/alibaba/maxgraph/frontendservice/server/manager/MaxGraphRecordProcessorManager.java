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
package com.alibaba.maxgraph.frontendservice.server.manager;

import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.frontendservice.Frontend;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.api.manager.RecordProcessorManager;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.structure.manager.record.AbstractEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.AddEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.AddVertexManager;
import com.alibaba.maxgraph.structure.manager.record.DelEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.DelVertexManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateEdgeManager;
import com.alibaba.maxgraph.structure.manager.record.UpdateVertexManager;
import com.alibaba.maxgraph.cache.CacheFactory;
import com.alibaba.maxgraph.structure.MxEdge;
import com.alibaba.maxgraph.structure.MxVertex;
import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.Iterator;

public class MaxGraphRecordProcessorManager implements RecordProcessorManager {
    private TinkerMaxGraph graph;
    private Frontend frontend;
    private Graph tinkerGraph;
    private Cache<ElementId, Vertex> vertexCache = CacheFactory.getCacheFactory().getVertexCache();

    public MaxGraphRecordProcessorManager(TinkerMaxGraph graph, Frontend frontend) {
        this.graph = graph;
        this.frontend = frontend;
        this.tinkerGraph = this.frontend.getGraph();
    }

    public Vertex addVertex(AddVertexManager addVertexManager) {
        MxVertex mxVertex = new MxVertex(
                graph.getBaseGraph()
                        .addVertex(
                                addVertexManager.label(),
                                addVertexManager.getPropertyList()),
                this.graph);
        Vertex vertex = DetachedFactory.detach(
                mxVertex,
                true);
        vertexCache.put((ElementId) vertex.id(), vertex);
        return vertex;
    }

    public void deleteVertex(DelVertexManager delVertexManager) {
        this.graph.getBaseGraph().deleteVertex(delVertexManager.getVertexId());
    }

    public Vertex updateVertex(UpdateVertexManager updateVertexManager) {
        throw new UnsupportedOperationException();
    }

    private Pair<com.alibaba.maxgraph.structure.Vertex, com.alibaba.maxgraph.structure.Vertex> getEdgeSrcDstPair(AbstractEdgeManager edgeManager) {
        ElementId srcId = edgeManager.getSrcId();
        ElementId dstId = edgeManager.getDstId();
        Iterator<com.alibaba.maxgraph.structure.Vertex> vertexList = this.graph.getBaseGraph().getVertex(Sets.newHashSet(srcId, dstId));
        com.alibaba.maxgraph.structure.Vertex srcVertex = null, dstVertex = null;
        while (vertexList.hasNext()) {
            com.alibaba.maxgraph.structure.Vertex vertex = vertexList.next();
            if (vertex.id.id() == srcId.id()) {
                srcVertex = vertex;
            }
            if (vertex.id.id() == dstId.id()) {
                dstVertex = vertex;
            }
        }
        if (srcVertex == null || dstVertex == null) {
            throw new IllegalArgumentException("cant get src or dst vertex for srcId[" + srcId + "] and dstId[" + dstId + "]");
        }

        return Pair.of(srcVertex, dstVertex);
    }

    public Edge addEdge(AddEdgeManager addEdgeManager) {
        Pair<com.alibaba.maxgraph.structure.Vertex, com.alibaba.maxgraph.structure.Vertex> vertexPair = getEdgeSrcDstPair(addEdgeManager);
        return DetachedFactory.detach(new MxEdge(
                        this.graph.getBaseGraph().addEdge(
                                addEdgeManager.getLabel(),
                                vertexPair.getLeft(),
                                vertexPair.getRight(),
                                addEdgeManager.getPropertyList()),
                        this.graph),
                false);
    }

    public void deleteEdge(DelEdgeManager delEdgeManager) {
        this.graph.getBaseGraph().deleteEdge(delEdgeManager.getLabel(), delEdgeManager.getEdgeId(), delEdgeManager.getSrcId(), delEdgeManager.getDstId());
    }

    public void updateEdge(UpdateEdgeManager updateEdgeManager) {
        try {
            Pair<com.alibaba.maxgraph.structure.Vertex, com.alibaba.maxgraph.structure.Vertex> vertexPair = getEdgeSrcDstPair(updateEdgeManager);
            this.graph.getBaseGraph().updateEdge(
                    vertexPair.getLeft(),
                    vertexPair.getRight(),
                    updateEdgeManager.getLabel(),
                    updateEdgeManager.getEdgeId(),
                    updateEdgeManager.getPropertyList());
        } catch (Exception e) {
            throw new RuntimeException("send data failed:" + CommonUtil.exceptionToString(e));
        }
    }
}
