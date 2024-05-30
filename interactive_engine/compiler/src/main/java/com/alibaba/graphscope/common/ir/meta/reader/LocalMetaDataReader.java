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
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// a local file system implementation of MetaDataReader
public class LocalMetaDataReader implements MetaDataReader {
    private static final Logger logger = LoggerFactory.getLogger(LocalMetaDataReader.class);
    private final Configs configs;

    public LocalMetaDataReader(Configs configs) {
        this.configs = configs;
    }

    @Override
    public List<InputStream> getStoredProcedures() throws IOException {
        String storedProceduresYaml = GraphConfig.GRAPH_STORED_PROCEDURES_YAML.get(configs);
        if (StringUtils.isEmpty(storedProceduresYaml)) {
            return ImmutableList.of();
        }
        Yaml yaml = new Yaml();
        Object raw = yaml.load(storedProceduresYaml);
        if (!(raw instanceof List)) {
            logger.error("stored procedures yaml format error");
            return ImmutableList.of();
        }
        List<Object> procedureList = (List<Object>) raw;
        return procedureList.stream()
                .map(
                        k -> {
                            String procedureYaml = yaml.dump(k);
                            return new ByteArrayInputStream(procedureYaml.getBytes());
                        })
                .collect(Collectors.toList());
    }

    @Override
    public SchemaInputStream getGraphSchema() throws IOException {
        String schemaPath =
                Objects.requireNonNull(
                        GraphConfig.GRAPH_SCHEMA.get(configs), "schema path not exist");
        return new SchemaInputStream(
                new FileInputStream(schemaPath), FileUtils.getFormatType(schemaPath));
    }

    @Override
    public SchemaInputStream getStatistics() throws Exception {
        String statisticsPath = GraphConfig.GRAPH_STATISTICS.get(configs);
        if (StringUtils.isEmpty(statisticsPath)) {
            return null;
        } else {
            return new SchemaInputStream(
                    new FileInputStream(statisticsPath), FileUtils.getFormatType(statisticsPath));
        }
    }
}
