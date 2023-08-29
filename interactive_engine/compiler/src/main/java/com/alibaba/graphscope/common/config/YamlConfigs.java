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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
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
                            String workspace = configs.get("directories.workspace");
                            String subdir = configs.get("directories.subdirs.data");
                            String graphName = configs.get("default_graph");
                            if (workspace != null && subdir != null && graphName != null) {
                                return Path.of(workspace, subdir, graphName, "graph.yaml")
                                        .toString();
                            } else {
                                return null;
                            }
                        })
                .put(
                        "graph.stored.procedures",
                        (Configs configs) -> {
                            String workspace = configs.get("directories.workspace");
                            String subdir = configs.get("directories.subdirs.data");
                            String graphName = configs.get("default_graph");
                            try {
                                if (workspace != null && subdir != null && graphName != null) {
                                    File schemaFile =
                                            new File(GraphConfig.GRAPH_SCHEMA.get(configs));
                                    if (!schemaFile.exists()
                                            || !schemaFile.getName().endsWith(".yaml")) {
                                        return null;
                                    }
                                    Yaml yaml = new Yaml();
                                    Map<String, Object> yamlAsMap =
                                            yaml.load(new FileInputStream(schemaFile));
                                    Object value;
                                    if ((value = yamlAsMap.get("stored_procedures")) == null
                                            || (value = ((Map) value).get("directory")) == null) {
                                        return null;
                                    }
                                    String directory = value.toString();
                                    return Path.of(workspace, subdir, graphName, directory)
                                            .toString();
                                } else {
                                    return null;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .put(
                        "graph.stored.procedures.enable.lists",
                        (Configs configs) -> {
                            File schemaFile = new File(GraphConfig.GRAPH_SCHEMA.get(configs));
                            if (!schemaFile.exists() || !schemaFile.getName().endsWith(".yaml")) {
                                return null;
                            }
                            try {
                                Yaml yaml = new Yaml();
                                Map<String, Object> yamlAsMap =
                                        yaml.load(new FileInputStream(schemaFile));
                                Object value;
                                if ((value = yamlAsMap.get("stored_procedures")) == null
                                        || (value = ((Map) value).get("enable_lists")) == null) {
                                    return null;
                                }
                                return value.toString().replace("[", "").replace("]", "");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .put(
                        "pegasus.worker.num",
                        (Configs configs) -> {
                            String type = configs.get("compute_engine.type");
                            if (type == null || !type.equals("pegasus")) {
                                return null;
                            } else {
                                return configs.get("compute_engine.worker_num");
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
                                String hosts = configs.get("compute_engine.hosts");
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
                                String hosts = configs.get("compute_engine.hosts");
                                if (hosts != null) {
                                    return hosts.replace("[", "").replace("]", "");
                                }
                                return hosts;
                            }
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
                .put("engine.type", (Configs configs) -> configs.get("compute_engine.type"));
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
