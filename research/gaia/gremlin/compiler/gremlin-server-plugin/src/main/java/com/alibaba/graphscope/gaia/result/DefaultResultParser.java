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
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.translator.builder.ConfigBuilder;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.GraphType;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.List;

public class DefaultResultParser implements ResultParser {
    private static final Logger logger = LoggerFactory.getLogger(DefaultResultParser.class);
    protected TagIdMaker tagIdMaker;
    protected GraphStoreService graphStore;
    protected GaiaConfig config;

    public DefaultResultParser(ConfigBuilder builder, GraphStoreService graphStore, GaiaConfig config) {
        this.tagIdMaker = (TagIdMaker) builder.getConfig(PlanConfig.TAG_ID_MAKER);
        this.graphStore = graphStore;
        this.config = config;
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
            Triple<String, String, String> labels = extractEdgeLabel(edge);
            return new DetachedEdge(extractEdgeId(edge), labels.getLeft(), extractProperties(edge),
                    edge.getSrcId(), labels.getMiddle(),
                    edge.getDstId(), labels.getRight());
        }
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            GremlinResult.Vertex vertex = elementPB.getVertex();
            return new DetachedVertex(vertex.getId(), extractVertexLabel(vertex.getLabel(), vertex.getId()),
                    extractProperties(vertex));
        }
        throw new RuntimeException("graph element type not set");
    }

    protected String extractVertexLabel(GremlinResult.Label label, long vertexId) {
        if (label.getItemCase() == GremlinResult.Label.ItemCase.NAME_ID) {
            return graphStore.getLabel(label.getNameId());
        } else {
            if (config.getGraphType() == GraphType.EXPERIMENTAL) {
                // pre 8 bits in vertex_id is label_id
                long labelId = (vertexId >> 56) & 0xff;
                return graphStore.getLabel(labelId);
            } else {
                throw new UnsupportedOperationException("cannot extract vertex label from returned vertex");
            }
        }
    }

    // return <edge label, label of source vertex, label of dst vertex>
    protected Triple<String, String, String> extractEdgeLabel(GremlinResult.Edge edge) {
        GremlinResult.Label edgeLabel = edge.getLabel();
        if (edgeLabel.getItemCase() != GremlinResult.Label.ItemCase.NAME_ID) {
            throw new UnsupportedOperationException("cannot extract edge label from returned edge");
        } else {
            String edgeLabelName = graphStore.getLabel(edgeLabel.getNameId());
            String srcLabelName = extractVertexLabel(edge.getSrcLabel(), edge.getSrcId());
            String dstLabelName = extractVertexLabel(edge.getDstLabel(), edge.getDstId());
            return Triple.of(edgeLabelName, srcLabelName, dstLabelName);
        }
    }

    protected Object extractEdgeId(GremlinResult.Edge edge) {
        return graphStore.fromBytes(edge.getId().toByteArray());
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
        entries.getPropertyList().forEach(p -> {
            String propertyName;
            if (p.getKey().getItemCase() == Common.PropertyKey.ItemCase.NAME_ID) {
                propertyName = graphStore.getPropertyName(p.getKey().getNameId());
            } else {
                propertyName = p.getKey().getName();
            }
            result.put(propertyName, parseValue(p.getValue()));
        });
        return result;
    }

    protected Map<String, Object> extractProperties(GremlinResult.Edge edge) {
        return Collections.EMPTY_MAP;
    }

    protected Map<String, Object> extractProperties(GremlinResult.Vertex vertex) {
        return Collections.EMPTY_MAP;
    }
}
