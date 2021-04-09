package com.alibaba.maxgraph.v2.frontend.compiler.rpc;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.frontend.result.EntryValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexResult;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.processor.MaxGraphProcessor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaxGraphGremlinResultProcessor implements MaxGraphResultProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphGremlinResultProcessor.class);
    private Context context;
    private int resultIterationSize;
    private SnapshotMaxGraph snapshotMaxGraph;
    private Map<Long, VertexResult> vertexResultMapping = Maps.newHashMap();
    private long resultCount = 0;

    public MaxGraphGremlinResultProcessor(Context context,
                                          int resultIterationSize,
                                          SnapshotMaxGraph snapshotMaxGraph) {
        this.context = context;
        this.resultIterationSize = resultIterationSize;
        this.snapshotMaxGraph = snapshotMaxGraph;
    }

    @Override
    public void finish() {
        MaxGraphProcessor.writeResultList(this.context, Lists.newArrayList(), ResponseStatusCode.SUCCESS);
    }

    @Override
    public long total() {
        return resultCount;
    }

    private void parseVertexList(Object result, Map<ElementId, List<VertexResult>> queryVertexList) {
        if (result instanceof VertexResult) {
            VertexResult vertexResult = (VertexResult) result;
            VertexResult cachedResult = vertexResultMapping.get(vertexResult.getId());
            if (null != cachedResult) {
                vertexResult.getProperties().putAll(cachedResult.getProperties());
            } else {
                ElementId elementId = new CompositeId(vertexResult.getId(), vertexResult.getLabelId());
                List<VertexResult> existVertexList = queryVertexList.computeIfAbsent(elementId, k -> Lists.newArrayList());
                existVertexList.add(vertexResult);
            }
        } else if (result instanceof List) {
            List<Object> list = (List<Object>) result;
            for (Object listObject : list) {
                parseVertexList(listObject, queryVertexList);
            }
        } else if (result instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) result;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                parseVertexList(entry.getKey(), queryVertexList);
                parseVertexList(entry.getValue(), queryVertexList);
            }
        } else if (result instanceof EntryValueResult) {
            EntryValueResult entry = (EntryValueResult) result;
            parseVertexList(entry.getKey(), queryVertexList);
            parseVertexList(entry.getValue(), queryVertexList);
        } else if (result instanceof PathResult) {
            PathResult pathResult = (PathResult) result;
            for (PathValueResult pathValueResult : pathResult.getPathValueResultList()) {
                parseVertexList(pathValueResult.getValue(), queryVertexList);
            }
        } else if (result instanceof PathValueResult) {
            PathValueResult pathValueResult = (PathValueResult) result;
            parseVertexList(pathValueResult.getValue(), queryVertexList);
        }
    }

    @Override
    public void process(List<Object> parseResponse) {
        logger.debug("Process response list " + parseResponse);
        Map<ElementId, List<VertexResult>> queryVertexList = Maps.newHashMap();
        List<Object> currentResultList = Lists.newArrayListWithCapacity(this.resultIterationSize);
        List<List<Object>> resultList = Lists.newArrayList();
        for (Object result : parseResponse) {
            resultCount += 1;
            parseVertexList(result, queryVertexList);
            currentResultList.add(transformResult(result));
            if (currentResultList.size() >= this.resultIterationSize) {
                resultList.add(currentResultList);
                currentResultList = Lists.newArrayListWithCapacity(this.resultIterationSize);
            }
        }
        if (!currentResultList.isEmpty()) {
            resultList.add(currentResultList);
        }
        if (!queryVertexList.isEmpty()) {
            Iterator<Vertex> vertexIterator = this.snapshotMaxGraph.getGraphReader().getVertices(queryVertexList.keySet());
            while (vertexIterator.hasNext()) {
                Vertex vertex = vertexIterator.next();
                ElementId id = (ElementId) vertex.id();
                List<VertexResult> vertexResults = queryVertexList.get(id);
                if (null != vertexResults) {
                    VertexResult cacheResult = null;
                    for (VertexResult vertexResult : vertexResults) {
                        Iterator<VertexProperty<Object>> vertexPropertyIterator = vertex.properties();
                        while (vertexPropertyIterator.hasNext()) {
                            VertexProperty<Object> vertexProperty = vertexPropertyIterator.next();
                            if (!vertexResult.getProperties().containsKey(vertexProperty.key())) {
                                vertexResult.getProperties().put(vertexProperty.key(), vertexProperty.value());
                            }
                        }
                        if (null == cacheResult) {
                            cacheResult = vertexResult;
                        }
                    }
                    vertexResultMapping.put(id.id(), cacheResult);
                }
            }
        }
        for (List<Object> aggregateList : resultList) {
            MaxGraphProcessor.writeResultList(this.context, aggregateList, ResponseStatusCode.PARTIAL_CONTENT);
        }
        resultList.clear();
    }
}
