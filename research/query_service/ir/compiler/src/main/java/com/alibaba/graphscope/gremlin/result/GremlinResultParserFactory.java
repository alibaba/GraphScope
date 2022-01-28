package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.alibaba.graphscope.gremlin.transform.OpArgTransformFactory;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum GremlinResultParserFactory implements GremlinResultParser {
    GRAPH_ELEMENT {
        @Override
        public Element parseFrom(IrResult.Results results) {
            IrResult.Element element = ParserUtils.getHeadEntry(results).getElement();
            Object graphElement = ParserUtils.parseElement(element);
            if (!(graphElement instanceof Element)) {
                throw new GremlinResultParserException(element.getInnerCase() + " is not Vertex or Edge type");
            }
            return (Element) graphElement;
        }
    },
    SINGLE_VALUE {
        @Override
        public Object parseFrom(IrResult.Results results) {
            IrResult.Element element = ParserUtils.getHeadEntry(results).getElement();
            return ParserUtils.parseElement(element);
        }
    },
    PROJECT_VALUE {
        // values("name") -> key: head, value: "marko"
        // valueMap("name") -> key: head, value: {name, "marko"}
        // select("a").by("name") -> key: head, value: "marko"
        // select("a", "b").by("name") -> key: a, value: "marko"; key: b, value: "josh"
        // select("a", "b").by(valueMap("name")) -> key: a, value: {name, "marko"}; key: b, value: {name, "josh"}
        @Override
        public Object parseFrom(IrResult.Results results) {
            IrResult.Record record = results.getRecord();
            Map<String, Object> projectResult = new HashMap<>();
            record.getColumnsList().forEach(column -> {
                String tag = getTagFromColumnKey(column.getNameOrId());
                Object parseElement = ParserUtils.parseElement(column.getEntry().getElement());
                if (parseElement instanceof Map) {
                    Map<List, Object> projectTags = (Map<List, Object>) parseElement;
                    projectTags.forEach((k, v) -> {
                        if (!(v instanceof EmptyValue)) {
                            String property = (String) k.get(1);
                            if (property.isEmpty()) {
                                throw new GremlinResultParserException("map value should have property key");
                            }
                            Map tagEntry = (Map) projectResult.computeIfAbsent(tag, k1 -> new HashMap<>());
                            tagEntry.put(property, Collections.singletonList(v));
                        }
                    });
                } else {
                    if (!(parseElement instanceof EmptyValue)) {
                        projectResult.put(tag, parseElement);
                    }
                }
            });
            if (projectResult.isEmpty()) {
                return EmptyValue.INSTANCE;
            } else if (projectResult.size() == 1) {
                return projectResult.entrySet().iterator().next().getValue();
            } else {
                return projectResult;
            }
        }

        // project_a
        // a_name
        // name
        // none -> head
        private String getTagFromColumnKey(OuterExpression.NameOrId columnKey) {
            if (columnKey.getItemCase() == OuterExpression.NameOrId.ItemCase.ITEM_NOT_SET) {
                return "";
            }
            String key = columnKey.getName();
            String[] tagProperty = key.split("_");
            if (tagProperty.length == 0) {
                throw new GremlinResultParserException("column key " + key + " is invalid");
            }
            // project self
            if (key.startsWith(OpArgTransformFactory.PROJECT_SELF_PREFIX)) {
                return tagProperty[1];
            } else if (tagProperty.length == 1) {
                // head
                return "";
            } else {
                return tagProperty[0];
            }
        }
    },
    GROUP {
        @Override
        public Map parseFrom(IrResult.Results results) {
            IrResult.Record record = results.getRecord();
            Object key = null;
            Object value = null;
            for (IrResult.Column column : record.getColumnsList()) {
                String alias = column.getNameOrId().getName();
                Object parseEntry = parseGroupEntry(column.getEntry());
                if (parseEntry instanceof EmptyValue) {
                    continue;
                }
                if (alias.startsWith(ArgUtils.groupKeys())) {
                    key = parseEntry;
                } else {
                    value = parseEntry;
                }
            }
            // key or value can be null
            Map data = new HashMap();
            data.put(key, value);
            return data;
        }

        private Object parseGroupEntry(IrResult.Entry entry) {
            switch (entry.getInnerCase()) {
                case ELEMENT:
                    return ParserUtils.parseElement(entry.getElement());
                case COLLECTION:
                    return ParserUtils.parseCollection(entry.getCollection());
                default:
                    throw new GremlinResultParserException(entry.getInnerCase() + " is invalid");
            }
        }
    },
    PATH_EXPAND {
        @Override
        public List<Element> parseFrom(IrResult.Results results) {
            throw new GremlinResultParserException("the whole path is unsupported yet");
        }
    },
    UNION {
        @Override
        public Object parseFrom(IrResult.Results results) {
            GremlinResultParser resultParser = inferFromIrResults(results);
            return resultParser.parseFrom(results);
        }

        // try to infer from the results
        private GremlinResultParser inferFromIrResults(IrResult.Results results) {
            int columns = results.getRecord().getColumnsList().size();
            logger.info("result is {}", results);
            if (columns == 1) {
                IrResult.Entry entry = ParserUtils.getHeadEntry(results);
                switch (entry.getInnerCase()) {
                    case ELEMENT:
                        IrResult.Element element = entry.getElement();
                        if (element.getInnerCase() == IrResult.Element.InnerCase.VERTEX ||
                                element.getInnerCase() == IrResult.Element.InnerCase.EDGE) {
                            return GRAPH_ELEMENT;
                        } else if (element.getInnerCase() == IrResult.Element.InnerCase.OBJECT) {
                            Common.Value value = element.getObject();
                            if (value.getItemCase() == Common.Value.ItemCase.PAIR_ARRAY) {  // project
                                return PROJECT_VALUE;
                            } else { // simple type
                                return SINGLE_VALUE;
                            }
                        } else { // todo: path expand in whole path
                            throw new GremlinResultParserException(element.getInnerCase() + " is unsupported yet");
                        }
                    case COLLECTION: // path()
                    default:
                        throw new GremlinResultParserException(entry.getInnerCase() + " is unsupported yet");
                }
            } else if (columns > 1) { // project or group
                IrResult.Column column = results.getRecord().getColumnsList().get(0);
                OuterExpression.NameOrId columnName = column.getNameOrId();
                if (columnName.getItemCase() == OuterExpression.NameOrId.ItemCase.NAME) {
                    String name = columnName.getName();
                    if (name.startsWith(ArgUtils.groupKeys()) || name.startsWith(ArgUtils.groupValues())) {
                        return GROUP;
                    } else {
                        return PROJECT_VALUE;
                    }
                } else {
                    throw new GremlinResultParserException(columnName.getItemCase() + " is invalid");
                }
            } else {
                throw new GremlinResultParserException("columns should not be empty");
            }
        }
    };

    private static Logger logger = LoggerFactory.getLogger(GremlinResultParserFactory.class);
}
