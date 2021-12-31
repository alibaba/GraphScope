package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gaia.proto.IrResult;
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
    VALUE_MAP {
        @Override
        public Map parseFrom(IrResult.Results results) {
            IrResult.Element element = ParserUtils.getHeadEntry(results).getElement();
            Map<List, Object> parseElement = (Map<List, Object>) ParserUtils.parseElement(element);
            Map<String, Object> data = new HashMap<>();
            // remove head
            parseElement.forEach((k, v) -> {
                List<String> keys = (List<String>) k;
                data.put(keys.get(1), v);
            });
            return data;
        }
    },
    PROJECT_TAG {
        @Override
        public Object parseFrom(IrResult.Results results) {
            IrResult.Element element = ParserUtils.getHeadEntry(results).getElement();
            Object parseElement = ParserUtils.parseElement(element);
            if (parseElement instanceof Map) {
                Map<String, Object> data = new HashMap<>();
                Map<List, Object> projectTags = (Map<List, Object>) parseElement;
                projectTags.forEach((k, v) -> {
                    String tag = (String) k.get(0);
                    String property = (String) k.get(1);
                    if (tag.isEmpty() && property.isEmpty()) {
                        throw new GremlinResultParserException("string array inside pair key is empty");
                    }
                    if (tag.isEmpty()) {
                        data.put(property, v);
                    } else if (property.isEmpty()) {
                        data.put(tag, v);
                    } else {
                        Map tagEntry = (Map) data.computeIfAbsent(tag, k1 -> new HashMap<>());
                        tagEntry.put(property, v);
                    }
                });
                return data;
            } else {
                return parseElement;
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
                    case COLLECTION:
                        data.put(alias, ParserUtils.parseCollection(entry.getCollection()));
                    default:
                        throw new GremlinResultParserException(entry.getInnerCase() + " is invalid");
                }
            });
            return data;
        }
    }
}
