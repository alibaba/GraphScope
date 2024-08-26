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

package com.alibaba.graphscope.common.config;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class YamlConfigs extends Configs {
    private static ImmutableMap<String, ValueGetter> valueGetterMap;

    static {
        ImmutableMap.Builder<String, ValueGetter> mapBuilder = ImmutableMap.builder();
        mapBuilder
                .put(
                        "graph.planner.is.on",
                        (Configs configs) -> configs.get("compiler.planner.is_on"))
                .put("graph.planner.opt", (Configs configs) -> configs.get("compiler.planner.opt"))
                .put(
                        "graph.planner.rules",
                        (Configs configs) -> {
                            String rules = configs.get("compiler.planner.rules");
                            if (rules != null) {
                                rules = rules.replace("[", "").replace("]", "");
                            }
                            return rules;
                        })
                .put(
                        "graph.schema",
                        (Configs configs) -> {
                            // if System.properties contains graph.schema, use it
                            String schema = System.getProperty("graph.schema");
                            if (schema != null) {
                                return schema;
                            }
                            return configs.get("compiler.meta.reader.schema.uri");
                        })
                .put(
                        "graph.statistics",
                        (Configs configs) -> {
                            String statistics = System.getProperty("graph.statistics");
                            if (statistics != null) {
                                return statistics;
                            }
                            return configs.get("compiler.meta.reader.statistics.uri");
                        })
                .put(
                        "graph.meta.schema.fetch.interval.ms",
                        (Configs configs) -> configs.get("compiler.meta.reader.schema.interval"))
                .put(
                        "graph.meta.statistics.fetch.interval.ms",
                        (Configs configs) ->
                                configs.get("compiler.meta.reader.statistics.interval"))
                .put(
                        "graph.store",
                        (Configs configs) -> {
                            if (configs.get("compute_engine.store.type") != null) {
                                return configs.get("compute_engine.store.type");
                            } else {
                                return "cpp-mcsr";
                            }
                        })
                .put(
                        "graph.physical.opt",
                        (Configs configs) -> {
                            if (configs.get("compiler.physical.opt.config") != null) {
                                return configs.get("compiler.physical.opt.config");
                            } else {
                                return "ffi"; // default proto
                            }
                        })
                .put(
                        "pegasus.worker.num",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("pegasus")) {
                                return null;
                            } else {
                                return configs.get("compute_engine.thread_num_per_worker");
                            }
                        })
                .put(
                        "pegasus.batch.size",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("pegasus")) {
                                return null;
                            } else {
                                return configs.get("compute_engine.batch_size");
                            }
                        })
                .put(
                        "pegasus.output.capacity",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("pegasus")) {
                                return null;
                            } else {
                                return configs.get("compute_engine.output_capacity");
                            }
                        })
                .put(
                        "pegasus.hosts",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("pegasus")) {
                                return null;
                            } else {
                                String hosts = configs.get("compute_engine.workers");
                                if (hosts != null) {
                                    return hosts.replace("[", "").replace("]", "");
                                }
                                return hosts;
                            }
                        })
                .put(
                        "hiactor.hosts",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("hiactor")) {
                                return null;
                            } else {
                                String port = configs.get("http_service.query_port");
                                String address = configs.get("http_service.default_listen_address");
                                if (port != null && address != null) {
                                    String hosts = address + ":" + port;
                                    return hosts.replace("[", "").replace("]", "");
                                }
                                return null;
                            }
                        })
                .put(
                        "interactive.admin.endpoint",
                        (Configs configs) -> {
                            String host = configs.get("http_service.default_listen_address");
                            String port = configs.get("http_service.admin_port");
                            if (host != null) {
                                if (port != null) {
                                    return "http://" + host + ":" + port;
                                }
                            }
                            return null;
                        })
                .put(
                        "interactive.query.endpoint",
                        (Configs configs) -> {
                            String host = configs.get("http_service.default_listen_address");
                            String port = configs.get("http_service.query_port");
                            if (host != null) {
                                if (port != null) {
                                    return "http://" + host + ":" + port;
                                }
                            }
                            return null;
                        })
                .put(
                        "neo4j.bolt.server.disabled",
                        (Configs configs) ->
                                configs.get("compiler.endpoint.bolt_connector.disabled"))
                .put(
                        "neo4j.bolt.server.port",
                        (Configs configs) -> configs.get("compiler.endpoint.bolt_connector.port"))
                .put(
                        "gremlin.server.disabled",
                        (Configs configs) ->
                                configs.get("compiler.endpoint.gremlin_connector.disabled"))
                .put(
                        "gremlin.server.port",
                        (Configs configs) ->
                                configs.get("compiler.endpoint.gremlin_connector.port"))
                .put(
                        "query.execution.timeout.ms",
                        (Configs configs) -> configs.get("compiler.query_timeout"))
                .put("engine.type", (Configs configs) -> configs.get("compute_engine.type"))
                .put(
                        "calcite.default.charset",
                        (Configs configs) -> configs.get("compiler.calcite_default_charset"))
                .put(
                        "gremlin.script.language.name",
                        (Configs configs) -> configs.get("compiler.gremlin_script_language_name"));
        valueGetterMap = mapBuilder.build();
    }

    public YamlConfigs(String file) throws IOException {
        this(file, FileLoadType.RELATIVE_PATH);
    }

    public YamlConfigs(String file, FileLoadType loadType) throws IOException {
        super(loadYamlAsMap(file, loadType));
    }

    private static Map<String, String> loadYamlAsMap(String file, FileLoadType loadType)
            throws IOException {
        InputStream inputStream =
                (loadType == FileLoadType.RELATIVE_PATH)
                        ? new FileInputStream(file)
                        : Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        Yaml yaml = new Yaml();
        Map<String, String> flattenKeyValues = Maps.newHashMap();
        flattenAndConvert(yaml.load(inputStream), flattenKeyValues, StringUtils.EMPTY);
        return flattenKeyValues;
    }

    private static void flattenAndConvert(
            Map<String, Object> data, Map<String, String> properties, String parentKey) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = parentKey.isEmpty() ? entry.getKey() : parentKey + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenAndConvert((Map<String, Object>) value, properties, key);
            } else {
                properties.put(key, value.toString());
            }
        }
    }

    @Override
    public String get(String name) {
        ValueGetter convertor = valueGetterMap.get(name);
        if (convertor != null) {
            return convertor.getValue(this);
        }
        return super.get(name);
    }

    @Override
    public String get(String name, String defaultValue) {
        ValueGetter convertor = valueGetterMap.get(name);
        if (convertor != null) {
            String value = convertor.getValue(this);
            return (value == null) ? defaultValue : value;
        }
        return super.get(name, defaultValue);
    }

    private interface ValueGetter {
        String getValue(Configs configs);
    }
}
