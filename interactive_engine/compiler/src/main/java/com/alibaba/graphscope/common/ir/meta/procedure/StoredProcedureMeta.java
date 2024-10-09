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

package com.alibaba.graphscope.common.ir.meta.procedure;

import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.schema.GSDataTypeConvertor;
import com.alibaba.graphscope.common.ir.meta.schema.GSDataTypeDesc;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphStatistics;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpec;
import com.alibaba.graphscope.common.ir.rex.RexProcedureCall;
import com.alibaba.graphscope.common.ir.tools.GraphPlanExecutor;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.*;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class StoredProcedureMeta {
    public static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final String name;
    private final RelDataType returnType;
    private final List<Parameter> parameters;
    private final Mode mode;
    private final String description;
    private final String extension;
    private final Map<String, Object> options;

    public StoredProcedureMeta(
            String name,
            Mode mode,
            String description,
            String extension,
            RelDataType returnType,
            List<Parameter> parameters,
            Map<String, Object> options) {
        this.name = name;
        this.mode = mode;
        this.description = description;
        this.extension = extension;
        this.returnType = returnType;
        this.parameters = Objects.requireNonNull(parameters);
        this.options = options;
    }

    public StoredProcedureMeta(
            Configs configs, String queryStr, RelDataType returnType, List<Parameter> parameters) {
        // For optional keys, construct a map and pass it to the constructor.
        this(
                Config.NAME.get(configs),
                Mode.valueOf(Config.MODE.get(configs)),
                Config.DESCRIPTION.get(configs),
                Config.EXTENSION.get(configs),
                returnType,
                parameters,
                ImmutableMap.of(
                        Config.TYPE.getKey(),
                        Config.TYPE.get(configs),
                        Config.QUERY.getKey(),
                        queryStr));
    }

    public String getName() {
        return name;
    }

    public RelDataType getReturnType() {
        return returnType;
    }

    public List<Parameter> getParameters() {
        return Collections.unmodifiableList(parameters);
    }

    public Object getOption(String key) {
        return options.getOrDefault(key, null);
    }

    @Override
    public String toString() {
        return "StoredProcedureMeta{"
                + "name='"
                + name
                + '\''
                + ", returnType="
                + returnType
                + ", parameters="
                + parameters
                + ", option="
                + options
                + '}';
    }

    public Mode getMode() {
        return this.mode;
    }

    public static class Parameter {
        private final String name;
        private final RelDataType dataType;

        public Parameter(String name, RelDataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        public String getName() {
            return name;
        }

        public RelDataType getDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            return "Parameter{" + "name='" + name + '\'' + ", dataType=" + dataType + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Parameter parameter = (Parameter) o;
            return Objects.equals(name, parameter.name)
                    && Objects.equals(dataType, parameter.dataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, dataType);
        }
    }

    public static class Serializer {
        public static void perform(StoredProcedureMeta meta, OutputStream outputStream)
                throws IOException {
            Yaml yaml = new Yaml();
            String mapStr = yaml.dump(createProduceMetaMap(meta));
            outputStream.write(mapStr.getBytes(StandardCharsets.UTF_8));
        }

        private static Map<String, Object> createProduceMetaMap(StoredProcedureMeta meta) {
            GSDataTypeConvertor<RelDataType> typeConvertor =
                    GSDataTypeConvertor.Factory.create(RelDataType.class, typeFactory);
            return ImmutableMap.of(
                    Config.NAME.getKey(),
                    meta.name,
                    Config.DESCRIPTION.getKey(),
                    meta.description,
                    Config.MODE.getKey(),
                    meta.mode.name(),
                    Config.EXTENSION.getKey(),
                    meta.extension,
                    "library",
                    String.format("lib%s%s", meta.name, meta.extension),
                    "params",
                    meta.parameters.stream()
                            .map(
                                    k ->
                                            ImmutableMap.of(
                                                    "name",
                                                    k.name,
                                                    "type",
                                                    typeConvertor
                                                            .convert(k.getDataType())
                                                            .getYamlDesc()))
                            .collect(Collectors.toList()),
                    "returns",
                    meta.returnType.getFieldList().stream()
                            .map(
                                    k ->
                                            ImmutableMap.of(
                                                    "name", k.getName(),
                                                    "type",
                                                            typeConvertor
                                                                    .convert(k.getType())
                                                                    .getYamlDesc()))
                            .collect(Collectors.toList()),
                    Config.TYPE.getKey(),
                    meta.options.get(Config.TYPE.getKey()),
                    Config.QUERY.getKey(),
                    meta.options.get(Config.QUERY.getKey()));
        }
    }

    public static class Deserializer {
        public static StoredProcedureMeta perform(InputStream inputStream) throws IOException {
            GSDataTypeConvertor<RelDataType> typeConvertor =
                    GSDataTypeConvertor.Factory.create(RelDataType.class, typeFactory);
            Yaml yaml = new Yaml();
            Map<String, Object> config = yaml.load(inputStream);
            return new StoredProcedureMeta(
                    getValue(Config.NAME, config),
                    Mode.valueOf(getValue(Config.MODE, config)),
                    getValue(Config.DESCRIPTION, config),
                    getValue(Config.EXTENSION, config),
                    createReturnType((List) config.get("returns"), typeConvertor),
                    createParameters((List) config.get("params"), typeConvertor),
                    ImmutableMap.of(
                            Config.TYPE.getKey(), getValue(Config.TYPE, config),
                            Config.QUERY.getKey(), getValue(Config.QUERY, config)));
        }

        private static <T> T getValue(
                com.alibaba.graphscope.common.config.Config<T> config,
                Map<String, Object> valueMap) {
            Object value = valueMap.get(config.getKey());
            return (value != null) ? (T) value : config.get(new Configs(ImmutableMap.of()));
        }

        private static RelDataType createReturnType(
                List config, GSDataTypeConvertor<RelDataType> typeConvertor) {
            List<RelDataTypeField> fields = Lists.newArrayList();
            if (config == null) {
                return new RelRecordType(fields);
            }
            Iterator iterator = config.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                Map<String, Object> field = (Map<String, Object>) iterator.next();
                fields.add(
                        new RelDataTypeFieldImpl(
                                (String) field.get("name"),
                                index,
                                typeConvertor.convert(
                                        new GSDataTypeDesc(
                                                (Map<String, Object>) field.get("type")))));
                ++index;
            }
            return new RelRecordType(fields);
        }

        private static List<StoredProcedureMeta.Parameter> createParameters(
                List config, GSDataTypeConvertor<RelDataType> typeConvertor) {
            List<StoredProcedureMeta.Parameter> parameters = Lists.newArrayList();
            if (config == null) {
                return parameters;
            }
            Iterator iterator = config.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> field = (Map<String, Object>) iterator.next();
                parameters.add(
                        new StoredProcedureMeta.Parameter(
                                (String) field.get("name"),
                                typeConvertor.convert(
                                        new GSDataTypeDesc(
                                                (Map<String, Object>) field.get("type")))));
            }
            return parameters;
        }
    }

    public enum Mode implements GraphPlanExecutor {
        READ,
        WRITE,
        SCHEMA {
            @Override
            public void execute(
                    GraphPlanner.Summary summary, IrMeta irMeta, ExecutionResponseListener listener)
                    throws Exception {
                RexProcedureCall procedureCall =
                        (RexProcedureCall) summary.getLogicalPlan().getProcedureCall();
                String metaProcedure = procedureCall.op.getName();
                String metaInJson;
                // call gs.procedure.meta.schema();
                if (metaProcedure.endsWith("schema")) {
                    metaInJson = irMeta.getSchema().getSchemaSpec(SchemaSpec.Type.FLEX_IN_JSON);
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode rootNode = mapper.readTree(metaInJson);
                    metaInJson = mapper.writeValueAsString(rootNode.get("schema"));
                } else if (metaProcedure.endsWith(
                        "statistics")) { // call gs.procedure.meta.statistics();
                    Preconditions.checkArgument(
                            irMeta instanceof IrMetaStats,
                            "cannot get statistics from ir meta, should be instance"
                                    + " of %s, but is %s",
                            IrMetaStats.class,
                            irMeta.getClass());
                    metaInJson =
                            ((IrGraphStatistics) ((IrMetaStats) irMeta).getStatistics())
                                    .getStatsJson();
                } else {
                    throw new IllegalArgumentException("invalid meta procedure: " + metaProcedure);
                }
                IrResult.Entry metaEntry =
                        IrResult.Entry.newBuilder()
                                .setElement(
                                        IrResult.Element.newBuilder()
                                                .setObject(
                                                        Common.Value.newBuilder()
                                                                .setStr(metaInJson)
                                                                .build())
                                                .build())
                                .build();
                listener.onNext(
                        IrResult.Record.newBuilder()
                                .addColumns(
                                        IrResult.Column.newBuilder().setEntry(metaEntry).build())
                                .build());
                listener.onCompleted();
            }
        }
    }

    public static class Config {
        public static final com.alibaba.graphscope.common.config.Config<String> NAME =
                com.alibaba.graphscope.common.config.Config.stringConfig("name", "default");
        public static final com.alibaba.graphscope.common.config.Config<String> DESCRIPTION =
                com.alibaba.graphscope.common.config.Config.stringConfig(
                        "description", "default desc");
        public static final com.alibaba.graphscope.common.config.Config<String> EXTENSION =
                com.alibaba.graphscope.common.config.Config.stringConfig("extension", ".so");
        public static final com.alibaba.graphscope.common.config.Config<String> MODE =
                com.alibaba.graphscope.common.config.Config.stringConfig("mode", "READ");
        // option configurations.
        public static final com.alibaba.graphscope.common.config.Config<String> TYPE =
                com.alibaba.graphscope.common.config.Config.stringConfig(
                        "type", "UNKNOWN"); // cypher or cpp
        public static final com.alibaba.graphscope.common.config.Config<String> QUERY =
                com.alibaba.graphscope.common.config.Config.stringConfig("query", "UNKNOWN");
    }
}
