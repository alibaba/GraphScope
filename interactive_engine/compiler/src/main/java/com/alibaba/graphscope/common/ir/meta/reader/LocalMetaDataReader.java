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

package com.alibaba.graphscope.common.ir.meta.reader;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.config.Utils;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// a local file system implementation of MetaDataReader
public class LocalMetaDataReader implements MetaDataReader {
    private static final Logger logger = LoggerFactory.getLogger(LocalMetaDataReader.class);
    private final Configs configs;

    public LocalMetaDataReader(Configs configs) {
        this.configs = configs;
    }

    @Override
    public List<InputStream> getStoredProcedures() throws IOException {
        String procedurePath = GraphConfig.GRAPH_STORED_PROCEDURES.get(configs);
        File procedureDir = new File(procedurePath);
        if (!procedureDir.exists() || !procedureDir.isDirectory()) {
            logger.warn("procedure path='{}' not exist or not a directory", procedurePath);
            return ImmutableList.of();
        }
        List<String> enableProcedureList =
                Utils.convertDotString(
                        GraphConfig.GRAPH_STORED_PROCEDURES_ENABLE_LISTS.get(configs));
        List<InputStream> procedureInputs = Lists.newArrayList();
        if (enableProcedureList.isEmpty()) {
            for (File file : procedureDir.listFiles()) {
                procedureInputs.add(new FileInputStream(file));
            }
        } else {
            Map<String, InputStream> procedureInputMap =
                    getProcedureNameWithInputStream(procedureDir);
            for (String enableProcedure : enableProcedureList) {
                InputStream enableInput = procedureInputMap.get(enableProcedure);
                Preconditions.checkArgument(
                        enableInput != null,
                        "can not find procedure with name=%s under directory=%s, candidates are %s",
                        enableProcedure,
                        procedureDir,
                        procedureInputMap.keySet());
                procedureInputs.add(enableInput);
            }
        }
        return Collections.unmodifiableList(procedureInputs);
    }

    private Map<String, InputStream> getProcedureNameWithInputStream(File procedureDir)
            throws IOException {
        Map<String, InputStream> procedureInputMap = Maps.newHashMap();
        for (File file : procedureDir.listFiles()) {
            String procedureName = getProcedureName(file);
            procedureInputMap.put(procedureName, new FileInputStream(file));
        }
        return procedureInputMap;
    }

    private String getProcedureName(File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file)) {
            Yaml yaml = new Yaml();
            Map<String, Object> map = yaml.load(inputStream);
            Object procedureName = map.get("name");
            Preconditions.checkArgument(
                    procedureName != null, "procedure name not exist in %s", file.getName());
            return procedureName.toString();
        }
    }

    @Override
    public SchemaInputStream getGraphSchema() throws IOException {
        String schemaPath =
                Objects.requireNonNull(
                        GraphConfig.GRAPH_SCHEMA.get(configs), "schema path not exist");
        return new SchemaInputStream(
                new FileInputStream(schemaPath), FileUtils.getFormatType(schemaPath));
    }
}
