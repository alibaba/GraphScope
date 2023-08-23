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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

// a local file system implementation of MetaDataReader
public class LocalMetaDataReader implements MetaDataReader {
    private static final Logger logger = LoggerFactory.getLogger(LocalMetaDataReader.class);
    private final Configs configs;

    public LocalMetaDataReader(Configs configs) {
        this.configs = configs;
    }

    @Override
    public List<InputStream> getStoredProcedures() throws FileNotFoundException {
        String procedurePath = GraphConfig.GRAPH_STORED_PROCEDURES.get(configs);
        File procedureDir = new File(procedurePath);
        if (!procedureDir.exists() || !procedureDir.isDirectory()) {
            logger.warn("procedure path {} not exist or not a directory", procedurePath);
            return ImmutableList.of();
        }
        List<InputStream> procedureInputs = Lists.newArrayList();
        for (File file : procedureDir.listFiles()) {
            if (file.getName().endsWith(".yaml")) {
                procedureInputs.add(new FileInputStream(file));
            }
        }
        return Collections.unmodifiableList(procedureInputs);
    }

    @Override
    public InputStream getGraphSchema() throws FileNotFoundException {
        String schemaPath =
                Objects.requireNonNull(
                        GraphConfig.GRAPH_SCHEMA.get(configs), "schema path not exist");
        return new FileInputStream(schemaPath);
    }
}
