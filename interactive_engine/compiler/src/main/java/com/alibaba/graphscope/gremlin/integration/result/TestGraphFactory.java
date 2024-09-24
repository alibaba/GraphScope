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

package com.alibaba.graphscope.gremlin.integration.result;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.utils.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;

public enum TestGraphFactory implements GraphProperties {
    EXPERIMENTAL {
        @Override
        public Map<String, Object> getProperties(Configs configs) {
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
    RUST_MCSR {
        @Override
        public Map<String, Object> getProperties(Configs configs) {
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
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1103806595072\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1103806595073\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1103806595074\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1103806595075\": {\n"
                            + "      \"weight\": 0.2\n"
                            + "    }\n"
                            + "  }\n"
                            + "}";
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    },
    GROOT {
        @Override
        public Map<String, Object> getProperties(Configs configs) {
            String json =
                    "{\n"
                            + "  \"vertex_properties\": {\n"
                            + "    \"7279245059266044353\": {\n"
                            + "      \"name\": \"marko\",\n"
                            + "      \"age\": 29\n"
                            + "    },\n"
                            + "    \"-4593408084868477921\": {\n"
                            + "      \"name\": \"vadas\",\n"
                            + "      \"age\": 27\n"
                            + "    },\n"
                            + "    \"7263461402022796979\": {\n"
                            + "      \"name\": \"lop\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"-8728313207994723275\": {\n"
                            + "      \"name\": \"josh\",\n"
                            + "      \"age\": 32\n"
                            + "    },\n"
                            + "    \"6632543798356094189\": {\n"
                            + "      \"name\": \"ripple\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"-5566731246168985051\": {\n"
                            + "      \"name\": \"peter\",\n"
                            + "      \"age\": 35\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"edge_properties\": {\n"
                            + "    \"1000000\": {\n"
                            + "      \"weight\": 0.5\n"
                            + "    },\n"
                            + "    \"1000001\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1000004\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1000003\": {\n"
                            + "      \"weight\": 1.0\n"
                            + "    },\n"
                            + "    \"1000002\": {\n"
                            + "      \"weight\": 0.4\n"
                            + "    },\n"
                            + "    \"1000005\": {\n"
                            + "      \"weight\": 0.2\n"
                            + "    }\n"
                            + "  }\n"
                            + "}";
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    },
    VINEYARD {
        @Override
        public Map<String, Object> getProperties(Configs configs) {
            String json;
            int servers = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
            if (servers == 1) {
                json =
                        "{\n"
                                + "  \"vertex_properties\": {\n"
                                + "    \"3\": {\n"
                                + "      \"name\": \"marko\",\n"
                                + "      \"age\": 29\n"
                                + "    },\n"
                                + "    \"0\": {\n"
                                + "      \"name\": \"vadas\",\n"
                                + "      \"age\": 27\n"
                                + "    },\n"
                                + "    \"72057594037927936\": {\n"
                                + "      \"name\": \"lop\",\n"
                                + "      \"lang\": \"java\"\n"
                                + "    },\n"
                                + "    \"2\": {\n"
                                + "      \"name\": \"josh\",\n"
                                + "      \"age\": 32\n"
                                + "    },\n"
                                + "    \"72057594037927937\": {\n"
                                + "      \"name\": \"ripple\",\n"
                                + "      \"lang\": \"java\"\n"
                                + "    },\n"
                                + "    \"1\": {\n"
                                + "      \"name\": \"peter\",\n"
                                + "      \"age\": 35\n"
                                + "    }\n"
                                + "  },\n"
                                + "  \"edge_properties\": {\n"
                                + "    \"0\": {\n"
                                + "      \"weight\": 0.5\n"
                                + "    },\n"
                                + "    \"72057594037927938\": {\n"
                                + "      \"weight\": 0.4\n"
                                + "    },\n"
                                + "    \"72057594037927937\": {\n"
                                + "      \"weight\": 1.0\n"
                                + "    },\n"
                                + "    \"72057594037927936\": {\n"
                                + "      \"weight\": 0.4\n"
                                + "    },\n"
                                + "    \"1\": {\n"
                                + "      \"weight\": 1.0\n"
                                + "    },\n"
                                + "    \"72057594037927939\": {\n"
                                + "      \"weight\": 0.2\n"
                                + "    }\n"
                                + "  }\n"
                                + "}";
            } else {
                json =
                        "{\n"
                                + "  \"vertex_properties\": {\n"
                                + "    \"-9223372036854775808\": {\n"
                                + "      \"name\": \"marko\",\n"
                                + "      \"age\": 29\n"
                                + "    },\n"
                                + "    \"1\": {\n"
                                + "      \"name\": \"vadas\",\n"
                                + "      \"age\": 27\n"
                                + "    },\n"
                                + "    \"-9151314442816847872\": {\n"
                                + "      \"name\": \"lop\",\n"
                                + "      \"lang\": \"java\"\n"
                                + "    },\n"
                                + "    \"0\": {\n"
                                + "      \"name\": \"josh\",\n"
                                + "      \"age\": 32\n"
                                + "    },\n"
                                + "    \"-9151314442816847871\": {\n"
                                + "      \"name\": \"ripple\",\n"
                                + "      \"lang\": \"java\"\n"
                                + "    },\n"
                                + "    \"2\": {\n"
                                + "      \"name\": \"peter\",\n"
                                + "      \"age\": 35\n"
                                + "    }\n"
                                + "  },\n"
                                + "  \"edge_properties\": {\n"
                                + "    \"0\": {\n"
                                + "      \"weight\": 0.5\n"
                                + "    },\n"
                                + "    \"72057594037927936\": {\n"
                                + "      \"weight\": 0.4\n"
                                + "    },\n"
                                + "    \"1\": {\n"
                                + "      \"weight\": 1.0\n"
                                + "    },\n"
                                + "    \"72057594037927938\": {\n"
                                + "      \"weight\": 0.4\n"
                                + "    },\n"
                                + "    \"72057594037927937\": {\n"
                                + "      \"weight\": 1.0\n"
                                + "    },\n"
                                + "    \"-9151314442816847872\": {\n"
                                + "      \"weight\": 0.2\n"
                                + "    }\n"
                                + "  }\n"
                                + "}";
            }
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    },
    CPP_MCSR {
        @Override
        public Map<String, Object> getProperties(Configs configs) {
            String json =
                    "{\n"
                            + "  \"vertex_properties\": {\n"
                            + "    \"0\": {\n"
                            + "      \"name\": \"marko\",\n"
                            + "      \"age\": 29\n"
                            + "    },\n"
                            + "    \"1\": {\n"
                            + "      \"name\": \"vadas\",\n"
                            + "      \"age\": 27\n"
                            + "    },\n"
                            + "    \"2\": {\n"
                            + "      \"name\": \"josh\",\n"
                            + "      \"age\": 32\n"
                            + "    },\n"
                            + "    \"3\": {\n"
                            + "      \"name\": \"peter\",\n"
                            + "      \"age\": 35\n"
                            + "    },\n"
                            + "    \"72057594037927936\": {\n"
                            + "      \"name\": \"lop\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    },\n"
                            + "    \"72057594037927937\": {\n"
                            + "      \"name\": \"ripple\",\n"
                            + "      \"lang\": \"java\"\n"
                            + "    }\n"
                            + "  },\n"
                            + "  \"edge_properties\": {\n"
                            + "    \"1103806595072\": {\n"
                            + "      \"weight\": 0.4\n" // 1-[created]->3
                            + "    },\n"
                            + "    \"1103808692224\": {\n"
                            + "      \"weight\": 0.4\n" // 4-[created]->3
                            + "    },\n"
                            + "    \"1103809740800\": {\n"
                            + "      \"weight\": 1.0\n" // 6-[created]->3
                            + "    },\n"
                            + "    \"1103808692225\": {\n"
                            + "      \"weight\": 0.2\n" // 4-[created]->5
                            + "    },\n"
                            + "    \"1\": {\n"
                            + "      \"weight\": 0.5\n" // 1-[knows]->2
                            + "    },\n"
                            + "    \"2\": {\n"
                            + "      \"weight\": 1.0\n" // 2-[knows]->4
                            + "    }\n"
                            + "  }\n"
                            + "}";
            return JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
        }
    }
}
