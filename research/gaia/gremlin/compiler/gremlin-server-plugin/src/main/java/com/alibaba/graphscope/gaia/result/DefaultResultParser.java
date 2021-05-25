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

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.translator.builder.ConfigBuilder;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.StaticGraphStore;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;

public class DefaultResultParser implements ResultParser {
    private static final Logger logger = LoggerFactory.getLogger(DefaultResultParser.class);
    private TagIdMaker tagIdMaker;
    protected final GraphStoreService graphStore = StaticGraphStore.INSTANCE;

    public DefaultResultParser(ConfigBuilder builder) {
        this.tagIdMaker = (TagIdMaker) builder.getConfig(PlanConfig.TAG_ID_MAKER);
    }

    @Override
    public List<Object> parseFrom(GremlinResult.Result resultData) {
        List<Object> result = new ArrayList<>();
        if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultData.getPaths().getItemList().forEach(p -> {
                result.add(parsePath(p));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultData.getElements().getItemList().forEach(e -> {
                result.add(parseElement(e));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.TAG_ENTRIES) {
            resultData.getTagEntries().getItemList().forEach(e -> {
                result.add(parseTagPropertyValue(e));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.MAP_RESULT) {
            resultData.getMapResult().getItemList().forEach(e -> {
                Map entry = Collections.singletonMap(parsePairElement(e.getFirst()), parsePairElement(e.getSecond()));
                result.add(entry);
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE) {
            result.add(parseValue(resultData.getValue()));
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE_LIST) {
            resultData.getValueList().getItemList().forEach(k -> {
                result.add(parseValue(k));
            });
        } else {
            throw new UnsupportedOperationException("");
        }
        return result;
    }

    protected Object parseElement(GremlinResult.GraphElement elementPB) {
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.EDGE) {
            GremlinResult.Edge edge = elementPB.getEdge();
            String edgeLabelName = graphStore.getLabel(Long.valueOf(edge.getLabel()));
            return new DetachedEdge(extractEdgeId(edge), edgeLabelName, extractProperties(edge),
                    edge.getSrcId(), extractVertexLabel(edge.getSrcId()),
                    edge.getDstId(), extractVertexLabel(edge.getDstId()));
        }
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            GremlinResult.Vertex vertex = elementPB.getVertex();
            String vertexLabelName = graphStore.getLabel(Long.valueOf(vertex.getLabel()));
            return new DetachedVertex(vertex.getId(), vertexLabelName, extractProperties(vertex));
        }
        throw new RuntimeException("graph element type not set");
    }

    protected String extractVertexLabel(long vertexId) {
        // pre 8 bits
        long labelId = (vertexId >> 56) & 0xff;
        return graphStore.getLabel(labelId);
    }

    protected BigInteger extractEdgeId(GremlinResult.Edge edge) {
        return new BigInteger(edge.getId());
    }

    protected BigInteger extractVertexId(GremlinResult.Vertex vertex) {
        return new BigInteger(String.valueOf(vertex.getId()));
    }

    protected Object parsePath(GremlinResult.Path pathPB) {
        List<Object> path = new ArrayList<>();
        pathPB.getPathList().forEach(p -> {
            path.add(parseElement(p));
        });
        return path;
    }

    protected Object parseTagPropertyValue(GremlinResult.TagEntries tagEntries) {
        Map<String, Object> result = new HashMap();
        int entriesSize = tagEntries.getEntriesCount();
        for (GremlinResult.TagEntry entry : tagEntries.getEntriesList()) {
            result.put(tagIdMaker.getTagById(entry.getTag()), parseOneTagValue(entry.getValue()));
        }
        if (entriesSize == 1) {
            // return result without any explicit tag
            return result.values().iterator().next();
        } else {
            return result;
        }
    }

    protected Object parseOneTagValue(GremlinResult.OneTagValue oneTagValue) {
        if (oneTagValue.getItemCase() == GremlinResult.OneTagValue.ItemCase.ELEMENT) {
            return parseElement(oneTagValue.getElement());
        } else if (oneTagValue.getItemCase() == GremlinResult.OneTagValue.ItemCase.VALUE) {
            return parseValue(oneTagValue.getValue());
        } else if (oneTagValue.getItemCase() == GremlinResult.OneTagValue.ItemCase.PROPERTIES) {
            return parseValueMap(oneTagValue.getProperties());
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected Object parsePairElement(GremlinResult.PairElement pairElement) {
        if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.GRAPH_ELEMENT) {
            return parseElement(pairElement.getGraphElement());
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.VALUE) {
            return parseValue(pairElement.getValue());
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.GRAPH_ELEMENT_LIST) {
            List<Object> result = new ArrayList();
            pairElement.getGraphElementList().getItemList().forEach(e -> {
                result.add(parseElement(e));
            });
            return result;
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.VALUE_LIST) {
            List<Object> result = new ArrayList();
            pairElement.getValueList().getItemList().forEach(e -> {
                result.add(parseValue(e));
            });
            return result;
        } else {
            throw new UnsupportedOperationException("parse pair element not support " + pairElement.getInnerCase());
        }
    }

    protected Object parseValue(Common.Value value) {
        if (value.getItemCase() == Common.Value.ItemCase.BOOLEAN) {
            return value.getBoolean();
        } else if (value.getItemCase() == Common.Value.ItemCase.I32) {
            return value.getI32();
        } else if (value.getItemCase() == Common.Value.ItemCase.I64) {
            return value.getI64();
        } else if (value.getItemCase() == Common.Value.ItemCase.F64) {
            return value.getF64();
        } else if (value.getItemCase() == Common.Value.ItemCase.STR) {
            return value.getStr();
        } else if (value.getItemCase() == Common.Value.ItemCase.BLOB) {
            return value.getBlob().toStringUtf8();
        } else if (value.getItemCase() == Common.Value.ItemCase.I32_ARRAY) {
            return value.getI32Array().getItemList();
        } else if (value.getItemCase() == Common.Value.ItemCase.F64_ARRAY) {
            return value.getF64Array().getItemList();
        } else if (value.getItemCase() == Common.Value.ItemCase.I64_ARRAY) {
            return value.getI64Array().getItemList();
        } else if (value.getItemCase() == Common.Value.ItemCase.STR_ARRAY) {
            return new ArrayList<>(value.getStrArray().getItemList());
        } else {
            throw new UnsupportedOperationException("parse value not support " + value.getItemCase());
        }
    }

    protected Map<String, Object> parseValueMap(GremlinResult.ValueMapEntries entries) {
        Map<String, Object> result = new HashMap<>();
        entries.getPropertyList().forEach(p -> result.put(p.getKey(), parseValue(p.getValue())));
        return result;
    }

    protected Map<String, Object> extractProperties(GremlinResult.Edge edge) {
        return Collections.EMPTY_MAP;
    }

    protected Map<String, Object> extractProperties(GremlinResult.Vertex vertex) {
        return Collections.EMPTY_MAP;
    }
}
