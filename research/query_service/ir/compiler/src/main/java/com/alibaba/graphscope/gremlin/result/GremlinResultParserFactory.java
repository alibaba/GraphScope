package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
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
        // values("name") -> key: name, value: "marko"
        // valueMap("name") -> key: {name}, value: {name, "marko"}
        // select("a").by("name") -> key: a, value: "marko"
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
        private String getTagFromColumnKey(OuterExpression.NameOrId columnKey) {
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
    };

    private static Logger logger = LoggerFactory.getLogger(GremlinResultParserFactory.class);
}
