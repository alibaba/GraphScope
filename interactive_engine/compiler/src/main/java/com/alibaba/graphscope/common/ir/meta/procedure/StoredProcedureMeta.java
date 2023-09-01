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

import com.alibaba.graphscope.common.config.Configs;
import com.google.common.collect.ImmutableBiMap;
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
    private static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    private final String name;
    private final RelDataType returnType;
    private final List<Parameter> parameters;
    private final Mode mode;
    private final String description;
    private final String extension;

    protected StoredProcedureMeta(
            String name,
            Mode mode,
            String description,
            String extension,
            RelDataType returnType,
            List<Parameter> parameters) {
        this.name = name;
        this.mode = mode;
        this.description = description;
        this.extension = extension;
        this.returnType = returnType;
        this.parameters = Objects.requireNonNull(parameters);
    }

    public StoredProcedureMeta(
            Configs configs, RelDataType returnType, List<Parameter> parameters) {
        this(
                Config.NAME.get(configs),
                Mode.valueOf(Config.MODE.get(configs)),
                Config.DESCRIPTION.get(configs),
                Config.EXTENSION.get(configs),
                returnType,
                parameters);
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
                + '}';
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
            return ImmutableBiMap.of(
                    "name",
                    meta.name,
                    "description",
                    meta.description,
                    "mode",
                    meta.mode.name(),
                    "extension",
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
                                                    Utils.typeToStr(k.getDataType())))
                            .collect(Collectors.toList()),
                    "returns",
                    meta.returnType.getFieldList().stream()
                            .map(
                                    k ->
                                            ImmutableMap.of(
                                                    "name", k.getName(),
                                                    "type", Utils.typeToStr(k.getType())))
                            .collect(Collectors.toList()));
        }
    }

    public static class Deserializer {
        public static StoredProcedureMeta perform(InputStream inputStream) throws IOException {
            Yaml yaml = new Yaml();
            Map<String, Object> config = yaml.load(inputStream);
            return new StoredProcedureMeta(
                    (String) config.get("name"),
                    Mode.valueOf((String) config.get("mode")),
                    (String) config.get("description"),
                    (String) config.get("extension"),
                    createReturnType((List) config.get("returns")),
                    createParameters((List) config.get("params")));
        }

        private static RelDataType createReturnType(List config) {
            List<RelDataTypeField> fields = Lists.newArrayList();
            Iterator iterator = config.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                Map<String, Object> field = (Map<String, Object>) iterator.next();
                fields.add(
                        new RelDataTypeFieldImpl(
                                (String) field.get("name"),
                                index,
                                Utils.strToType((String) field.get("type"), typeFactory)));
                ++index;
            }
            return new RelRecordType(fields);
        }

        private static List<StoredProcedureMeta.Parameter> createParameters(List config) {
            List<StoredProcedureMeta.Parameter> parameters = Lists.newArrayList();
            Iterator iterator = config.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> field = (Map<String, Object>) iterator.next();
                parameters.add(
                        new StoredProcedureMeta.Parameter(
                                (String) field.get("name"),
                                Utils.strToType((String) field.get("type"), typeFactory)));
            }
            return parameters;
        }
    }

    public enum Mode {
        READ,
        WRITE,
        SCHEMA
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
    }
}
