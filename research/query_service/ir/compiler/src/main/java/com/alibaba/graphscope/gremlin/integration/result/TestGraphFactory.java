package com.alibaba.graphscope.gremlin.integration.result;

import com.alibaba.graphscope.common.utils.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;

public enum TestGraphFactory implements GraphProperties {
    EXPERIMENTAL {
        @Override
        public Map<String, Object> getProperties() {
            String json =
                    "{\n"
                            + "  \"vertex_properties\": {\n"
                            + "    \"1\": {\n"
                            + "      \"name\": \"marko\",\n"
                            + "      \"age\": 29\n"
                            + "    },\n"
                            + "    \"2\": {\n"
                            + "      \"name\": \"vadas\",\n"
                            + "      \"age\": 27\n"
                            + "    },\n"
                            + "    \"72057594037927939\": {\n"
                            + "      \"name\": \"lop\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"4\": {\n"
                            + "      \"name\": \"josh\",\n"
                            + "      \"age\": 32\n"
                            + "    },\n"
                            + "    \"72057594037927941\": {\n"
                            + "      \"name\": \"ripple\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"6\": {\n"
                            + "      \"name\": \"peter\",\n"
                            + "      \"age\": 35\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"edge_properties\": {\n"
                            + "    \"0\": {\n"
                            + "      \"weight\": 0.5\n"
                            + "    },\n"
                            + "    \"1\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"2\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"3\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"4\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"5\": {\n"
                            + "      \"weight\": 0.2\n"
                            + "    }\n"
                            + "  }\n"
                            + "}";
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    },
    GROOT {
        @Override
        public Map<String, Object> getProperties() {
            String json =
                    "{\n"
                            + "  \"vertex_properties\": {\n"
                            + "    \"-7732428334775821489\": {\n"
                            + "      \"name\": \"marko\",\n"
                            + "      \"age\": 29\n"
                            + "    },\n"
                            + "    \"6308168136910223060\": {\n"
                            + "      \"name\": \"vadas\",\n"
                            + "      \"age\": 27\n"
                            + "    },\n"
                            + "    \"-7991964441648465618\": {\n"
                            + "      \"name\": \"lop\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"-6112228345218519679\": {\n"
                            + "      \"name\": \"josh\",\n"
                            + "      \"age\": 32\n"
                            + "    },\n"
                            + "    \"2233628339503041259\": {\n"
                            + "      \"name\": \"ripple\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"-2045066182110421307\": {\n"
                            + "      \"name\": \"peter\",\n"
                            + "      \"age\": 35\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"edge_properties\": {\n"
                            + "    \"1000000\": {\n"
                            + "      \"weight\": 0.5\n"
                            + "    },\n"
                            + "    \"1000001\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1000004\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1000003\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1000002\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1000005\": {\n"
                            + "      \"weight\": 0.2\n"
                            + "    }\n"
                            + "  }\n"
                            + "}";
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    },
    UNKNOWN {
        @Override
        public Map<String, Object> getProperties() {
            throw new UnsupportedOperationException();
        }
    }
}
