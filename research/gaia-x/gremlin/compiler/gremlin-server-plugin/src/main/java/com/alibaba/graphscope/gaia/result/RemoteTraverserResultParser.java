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
package com.alibaba.graphscope.gaia.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.ConfigBuilder;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RemoteTraverserResultParser extends DefaultResultParser {
    private static final Logger logger = LoggerFactory.getLogger(RemoteTraverserResultParser.class);

    public RemoteTraverserResultParser(ConfigBuilder builder, GraphStoreService graphStore, GaiaConfig config) {
        super(builder, graphStore, config);
    }

    @Override
    public List<Object> parseFrom(GremlinResult.Result resultData) {
        List<Object> result = new ArrayList<>();
        if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultData.getPaths().getItemList().forEach(p -> {
                result.add(transform(parsePath(p)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultData.getElements().getItemList().forEach(e -> {
                result.add(transform(parseElement(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.TAG_ENTRIES) {
            resultData.getTagEntries().getItemList().forEach(e -> {
                result.add(transform(parseTagPropertyValue(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.MAP_RESULT) {
            resultData.getMapResult().getItemList().forEach(e -> {
                Map entry = Collections.singletonMap(parsePairElement(e.getFirst()), parsePairElement(e.getSecond()));
                result.add(transform(entry));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE) {
            result.add(transform(parseValue(resultData.getValue())));
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE_LIST) {
            resultData.getValueList().getItemList().forEach(k -> {
                result.add(transform(parseValue(k)));
            });
        } else {
            throw new UnsupportedOperationException("");
        }
        return result;
    }

    @Override
    protected Path parsePath(GremlinResult.Path pathPB) {
        Path path = MutablePath.make();
        pathPB.getPathList().forEach(p -> {
            path.extend(parseElement(p), Collections.EMPTY_SET);
        });
        return path;
    }

    private RemoteTraverser transform(Object object) {
        return new DefaultRemoteTraverser(object, 1);
    }

    @Override
    protected Map<String, Object> extractProperties(GremlinResult.Edge edge) {
        Map<String, Object> result = new HashMap<>();
        Set<String> keys = graphStore.getEdgeKeys(edge);
        for (String key : keys) {
            Optional propertyOpt = graphStore.getEdgeProperty(edge, key);
            if (propertyOpt.isPresent()) {
                result.put(key, propertyOpt.get());
            }
        }
        return result;
    }

    @Override
    protected Map<String, Object> extractProperties(GremlinResult.Vertex vertex) {
        Map<String, Object> result = new HashMap<>();
        Set<String> keys = graphStore.getVertexKeys(extractVertexId(vertex));
        for (String key : keys) {
            Optional propertyOpt = graphStore.getVertexProperty(extractVertexId(vertex), key);
            if (propertyOpt.isPresent()) {
                result.put(key, Collections.singletonList(ImmutableMap.of("id", 1L, "value", propertyOpt.get())));
            }
        }
        return result;
    }
}
