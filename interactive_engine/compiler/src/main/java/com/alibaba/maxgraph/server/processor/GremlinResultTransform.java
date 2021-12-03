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
package com.alibaba.maxgraph.server.processor;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.result.EdgeResult;
import com.alibaba.maxgraph.result.ListResult;
import com.alibaba.maxgraph.result.MapValueResult;
import com.alibaba.maxgraph.result.PathValueResult;
import com.alibaba.maxgraph.result.PropertyResult;
import com.alibaba.maxgraph.result.PropertyValueResult;
import com.alibaba.maxgraph.result.VertexPropertyResult;
import com.alibaba.maxgraph.result.VertexResult;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.sdkcommon.graph.EntryValueResult;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.alibaba.maxgraph.server.query.RemoteRpcConnector;
import com.alibaba.maxgraph.server.query.RemoteRpcProcessor;
import com.alibaba.maxgraph.server.query.RpcProcessorType;
import com.alibaba.maxgraph.cache.CacheFactory;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.MxVertex;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GremlinResultTransform {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResultTransform.class);

    private final RemoteRpcConnector remoteRpcConnector;
    private final RemoteRpcProcessor remoteRpcProcessor;
    private final TinkerMaxGraph graph;
    private final boolean vertexCacheFlag;
    private final ValueType resultValueType;
    private Cache<ElementId, org.apache.tinkerpop.gremlin.structure.Vertex> vertexCache;

    public GremlinResultTransform(
            RemoteRpcConnector remoteRpcConnector,
            RemoteRpcProcessor remoteRpcProcessor,
            TinkerMaxGraph graph,
            ValueType resultValueType,
            boolean vertexCacheFlag) {
        this.remoteRpcConnector = remoteRpcConnector;
        this.remoteRpcProcessor = remoteRpcProcessor;
        this.graph = graph;
        this.resultValueType = resultValueType;
        this.vertexCacheFlag = vertexCacheFlag;
        if (this.vertexCacheFlag) {
            vertexCache = CacheFactory.getCacheFactory().getVertexCache();
        }
    }

    private Object getPropertyValue(QueryResult queryResult) {
        if (queryResult instanceof PropertyValueResult) {
            return PropertyValueResult.class.cast(queryResult).getValue();
        } else if (queryResult instanceof PropertyResult) {
            return PropertyResult.class.cast(queryResult).getValue();
        } else {
            return VertexPropertyResult.class.cast(queryResult).value();
        }
    }

    private Object parseResultValue(
            QueryResult queryResult,
            GraphSchema schema,
            Graph graph,
            Map<Integer, String> labelIndexNameList,
            Context context,
            int batchSize,
            String queryId) {
        if (queryResult instanceof VertexPropertyResult || queryResult instanceof PropertyResult) {
            return queryResult;
        } else if (queryResult instanceof MapValueResult) {
            Map<Object, Object> resultMap = Maps.newLinkedHashMap();
            for (Map.Entry<QueryResult, QueryResult> entry :
                    ((MapValueResult) queryResult).getValueMap().entrySet()) {
                resultMap.put(
                        parseResultValue(
                                entry.getKey(),
                                schema,
                                graph,
                                labelIndexNameList,
                                context,
                                batchSize,
                                queryId),
                        parseResultValue(
                                entry.getValue(),
                                schema,
                                graph,
                                labelIndexNameList,
                                context,
                                batchSize,
                                queryId));
            }
            return resultMap;
        } else if (queryResult instanceof EdgeResult) {
            return DetachedFactory.detach((Edge) queryResult, true);
        } else if (queryResult instanceof ListResult) {
            List<QueryResult> resultList = ((ListResult) queryResult).getResultList();
            if (resultList.isEmpty()) {
                return Lists.newArrayList();
            }
            if (resultList.get(0) instanceof PathValueResult) {
                Path path = MutablePath.make();
                resultList.forEach(
                        v -> {
                            PathValueResult pathValueResult = PathValueResult.class.cast(v);
                            Set<String> labelNameList = Sets.newHashSet();
                            pathValueResult
                                    .getLabelIdList()
                                    .forEach(
                                            labelId ->
                                                    labelNameList.add(
                                                            labelIndexNameList.get(labelId)));
                            path.extend(
                                    parseResultValue(
                                            pathValueResult.getValue(),
                                            schema,
                                            graph,
                                            labelIndexNameList,
                                            context,
                                            batchSize,
                                            queryId),
                                    labelNameList);
                        });
                return DetachedFactory.detach(path, true);
            } else {
                List<Object> listValue = Lists.newArrayList();
                resultList.forEach(
                        v ->
                                listValue.add(
                                        parseResultValue(
                                                v,
                                                schema,
                                                graph,
                                                labelIndexNameList,
                                                context,
                                                batchSize,
                                                queryId)));
                return listValue;
            }
        } else if (queryResult instanceof EntryValueResult) {
            Map<Object, Object> mapValue = Maps.newHashMap();
            mapValue.put(
                    parseResultValue(
                            ((EntryValueResult) queryResult).getKey(),
                            schema,
                            graph,
                            labelIndexNameList,
                            context,
                            batchSize,
                            queryId),
                    parseResultValue(
                            ((EntryValueResult) queryResult).getValue(),
                            schema,
                            graph,
                            labelIndexNameList,
                            context,
                            batchSize,
                            queryId));
            return mapValue.entrySet().iterator().next();
        } else if (queryResult instanceof PropertyValueResult) {
            return ((PropertyValueResult) queryResult).getValue();
        } else if (queryResult instanceof VertexResult) {
            VertexResult vertexResult = (VertexResult) queryResult;
            CompositeId compositeId = CompositeId.class.cast(vertexResult.id());
            org.apache.tinkerpop.gremlin.structure.Vertex cachedVertex =
                    vertexCache.getIfPresent(compositeId);
            if (null == cachedVertex) {
                List<Object> resultList = Lists.newArrayList();
                Map<ElementId, Integer> elementIdIntegerMap = Maps.newHashMap();
                elementIdIntegerMap.put(compositeId, 1);
                Map<CompositeId, Map<String, Object>> existPropMap = Maps.newHashMap();
                extractExistProp(existPropMap, vertexResult, compositeId);
                Map<Integer, Set<ElementId>> storeVertexList = Maps.newHashMap();
                storeVertexList.put(vertexResult.getStoreId(), Sets.newHashSet(compositeId));
                queryVertices(
                        schema,
                        context,
                        batchSize,
                        resultList,
                        elementIdIntegerMap,
                        storeVertexList,
                        existPropMap,
                        RpcProcessorType.MEMORY,
                        queryId);
                cachedVertex =
                        org.apache.tinkerpop.gremlin.structure.Vertex.class.cast(resultList.get(0));
            }
            return cachedVertex;
        } else {
            return queryResult;
        }
    }

    public void transform(
            List<QueryResult> resultObjectList,
            GraphSchema schema,
            Map<Integer, String> labelIndexNameList,
            Context context,
            int batchSize,
            List<Object> resultList,
            String queryId) {
        Map<ElementId, Integer> vertexCountList = Maps.newHashMap();
        List<CompositeId> resultVertexIdList =
                Lists.newArrayListWithCapacity(resultObjectList.size());
        Map<CompositeId, org.apache.tinkerpop.gremlin.structure.Vertex> idToVertexList =
                Maps.newHashMap();
        Map<CompositeId, Map<String, Object>> existPropMap = Maps.newHashMap();
        Map<Integer, Set<ElementId>> storeVertexList = Maps.newHashMap();
        for (int i = 0; i < resultObjectList.size(); i++) {
            QueryResult queryResult = resultObjectList.get(i);
            if (queryResult instanceof VertexResult) {
                VertexResult vertexResult = VertexResult.class.cast(queryResult);
                CompositeId compositeId =
                        new CompositeId(
                                vertexResult.id,
                                schema.getElement(vertexResult.label).getLabelId());
                org.apache.tinkerpop.gremlin.structure.Vertex cachedVertex =
                        vertexCache.getIfPresent(compositeId);
                if (null != cachedVertex) {
                    resultVertexIdList.add(compositeId);
                    idToVertexList.put(compositeId, cachedVertex);
                } else {
                    resultVertexIdList.add(compositeId);
                    vertexCountList.compute(
                            compositeId,
                            (key, oldValue) -> {
                                if (null == oldValue) {
                                    return 1;
                                } else {
                                    return oldValue + 1;
                                }
                            });
                    Set<ElementId> storeVertices =
                            storeVertexList.computeIfAbsent(
                                    vertexResult.getStoreId(), k -> Sets.newHashSet());
                    storeVertices.add(compositeId);

                    extractExistProp(existPropMap, vertexResult, compositeId);
                }
            } else {
                Object result =
                        parseResultValue(
                                queryResult,
                                schema,
                                this.graph,
                                labelIndexNameList,
                                context,
                                batchSize,
                                queryId);
                if (null != resultList) {
                    resultList.add(result);
                }
                remoteRpcProcessor.process(result);
            }
        }
        if (!vertexCountList.isEmpty()) {
            List<Object> currResultList = Lists.newArrayListWithCapacity(vertexCountList.size());
            queryVertices(
                    schema,
                    context,
                    batchSize,
                    currResultList,
                    vertexCountList,
                    storeVertexList,
                    existPropMap,
                    RpcProcessorType.MEMORY,
                    queryId);
            for (Object currResult : currResultList) {
                org.apache.tinkerpop.gremlin.structure.Vertex currVertex =
                        org.apache.tinkerpop.gremlin.structure.Vertex.class.cast(currResult);
                CompositeId vertexId = CompositeId.class.cast(currVertex.id());
                idToVertexList.put(vertexId, currVertex);
            }
        }
        if (!resultVertexIdList.isEmpty()) {
            for (CompositeId compositeId : resultVertexIdList) {
                org.apache.tinkerpop.gremlin.structure.Vertex resultVertex =
                        idToVertexList.get(compositeId);
                remoteRpcProcessor.process(resultVertex);
                if (null != resultList) {
                    resultList.add(resultVertex);
                }
            }
        }
    }

    private void extractExistProp(
            Map<CompositeId, Map<String, Object>> existPropMap,
            VertexResult vertexResult,
            CompositeId compositeId) {
        if (!vertexResult.getPropertyList().isEmpty()) {
            Map<String, Object> propMap = Maps.newHashMap();
            for (VertexPropertyResult vpr : vertexResult.getPropertyList()) {
                propMap.put(vpr.key(), vpr.value());
            }
            existPropMap.put(compositeId, propMap);
        }
    }

    private void queryVertices(
            GraphSchema schema,
            Context context,
            int batchSize,
            List<Object> resultList,
            Map<ElementId, Integer> vertexCountList,
            Map<Integer, Set<ElementId>> classified,
            Map<CompositeId, Map<String, Object>> existPropMap,
            RpcProcessorType rpcProcessorType,
            String queryId) {
        logger.info("Start to fetch detail for vertex in query " + queryId);
        Map<Integer, Map<ElementId, Integer>> classifiedList = Maps.newHashMap();
        if (classified.size() == 1) {
            int key = Lists.newArrayList(classified.keySet()).get(0);
            classifiedList.put(key, vertexCountList);
        } else {
            for (Map.Entry<Integer, Set<ElementId>> entry : classified.entrySet()) {
                Map<ElementId, Integer> classifiedCountList = Maps.newHashMap();
                for (ElementId elementId : entry.getValue()) {
                    classifiedCountList.put(elementId, vertexCountList.remove(elementId));
                }
                classifiedList.put(entry.getKey(), classifiedCountList);
            }
        }
        if (null == rpcProcessorType) {
            remoteRpcConnector.queryVertices(
                    classifiedList,
                    schema,
                    graph,
                    context,
                    batchSize,
                    resultList,
                    vertexCacheFlag,
                    existPropMap);
        } else {
            remoteRpcConnector.queryVertices(
                    classifiedList,
                    schema,
                    graph,
                    context,
                    batchSize,
                    resultList,
                    vertexCacheFlag,
                    rpcProcessorType,
                    existPropMap);
        }

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
                if (null != resultList) {
                    resultList.add(vertex);
                }
                if (rpcProcessorType != RpcProcessorType.MEMORY) {
                    remoteRpcProcessor.process(vertex);
                }
            }
        }
        logger.info("Fetch detail for vertex in query " + queryId + " finish");
    }

    public void finish() {
        remoteRpcProcessor.finish(ResponseStatusCode.SUCCESS);
    }
}
