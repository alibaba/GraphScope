package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.alibaba.graphscope.gremlin.OpArgTransformFactory;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import org.apache.tinkerpop.gremlin.structure.Element;

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
                        String property = (String) k.get(1);
                        if (property.isEmpty()) {
                            throw new GremlinResultParserException("map value should have property key");
                        }
                        Map tagEntry = (Map) projectResult.computeIfAbsent(tag, k1 -> new HashMap<>());
                        tagEntry.put(property, v);
                    });
                } else {
                    projectResult.put(tag, parseElement);
                }
            });
            if (projectResult.size() == 1) {
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
        public Object parseFrom(IrResult.Results results) {
            IrResult.Record record = results.getRecord();
            Map data = new HashMap();
            record.getColumnsList().forEach(column -> {
                String alias = column.getNameOrId().getName();
                IrResult.Entry entry = column.getEntry();
                switch (entry.getInnerCase()) {
                    case ELEMENT:
                        data.put(alias, ParserUtils.parseElement(entry.getElement()));
                        break;
                    case COLLECTION:
                        data.put(alias, ParserUtils.parseCollection(entry.getCollection()));
                        break;
                    default:
                        throw new GremlinResultParserException(entry.getInnerCase() + " is invalid");
                }
            });
            return data;
        }
    };
}
