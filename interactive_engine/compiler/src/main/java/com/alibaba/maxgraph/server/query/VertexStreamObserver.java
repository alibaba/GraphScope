/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.proto.GremlinQuery;
import com.alibaba.maxgraph.sdk.exception.ExceptionHolder;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.cache.CacheFactory;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.MxVertex;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class VertexStreamObserver implements StreamObserver<GremlinQuery.VertexResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(VertexStreamObserver.class);
    private final Map<CompositeId, Map<String, Object>> existPropMap;

    private RemoteRpcProcessor resultProcessor;
    private GraphSchema schema;
    private TinkerMaxGraph graph;
    private ExceptionHolder exceptionHolder;
    private CountDownLatch latch;
    private Map<ElementId, Integer> vertexCountList;
    private boolean vertexCacheFlag;
    private Cache<ElementId, org.apache.tinkerpop.gremlin.structure.Vertex> vertexCache;

    public VertexStreamObserver(
            RemoteRpcProcessor resultProcessor,
            GraphSchema schema,
            TinkerMaxGraph graph,
            ExceptionHolder exceptionHolder,
            CountDownLatch latch,
            Map<ElementId, Integer> vertexCountList,
            boolean vertexCacheFlag,
            Map<CompositeId, Map<String, Object>> existPropMap) {
        this.resultProcessor = resultProcessor;
        this.schema = schema;
        this.graph = graph;
        this.exceptionHolder = exceptionHolder;
        this.latch = latch;
        this.vertexCountList = vertexCountList;
        this.vertexCacheFlag = vertexCacheFlag;
        if (this.vertexCacheFlag) {
            vertexCache = CacheFactory.getCacheFactory().getVertexCache();
        }
        this.existPropMap = existPropMap;
    }

    @Override
    public void onNext(GremlinQuery.VertexResponse vertexResponse) {
        GremlinQuery.VertexId id = vertexResponse.getId();
        CompositeId rId = new CompositeId(id.getId(), id.getTypeId());
        GraphElement type = schema.getElement(id.getTypeId());
        Map<String, Object> properties = null;
        try {
            properties =
                    RpcProcessorUtils.deserializeProperty(
                            vertexResponse.getPros().toByteArray(), type, schema);
        } catch (Exception e) {
            throw new RuntimeException(
                    "query properties for vertex=>" + rId.toString() + " fail", e);
        }

        Map<String, Object> existProp = existPropMap.get(rId);
        if (null != existProp) {
            if (properties == null) {
                properties = Maps.newHashMap();
            }
            properties.putAll(existProp);
        }

        MxVertex vertex =
                new MxVertex(
                        new Vertex(rId, type.getLabel(), properties, this.graph.getBaseGraph()),
                        this.graph);
        if (vertexCacheFlag) {
            vertexCache.put(rId, vertex);
        }
        int count = vertexCountList.remove(rId);
        if (count > 1) {
            for (int i = 0; i < count; i++) {
                resultProcessor.process(vertex);
            }
        } else {
            resultProcessor.process(vertex);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("error query count=>" + latch.getCount() + "s", throwable);
        exceptionHolder.hold(throwable);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        if (!vertexCountList.isEmpty()) {
            for (Map.Entry<ElementId, Integer> entry : vertexCountList.entrySet()) {
                ElementId elementId = entry.getKey();
                MxVertex vertex =
                        new MxVertex(
                                new Vertex(
                                        elementId,
                                        schema.getElement(elementId.typeId()).getLabel(),
                                        Maps.newHashMap(),
                                        this.graph.getBaseGraph()),
                                this.graph);
                if (vertexCacheFlag) {
                    vertexCache.put(elementId, vertex);
                }
                for (int i = 0; i < entry.getValue(); i++) {
                    resultProcessor.process(vertex);
                }
            }
        }
        resultProcessor.finish();
        latch.countDown();
    }
}
