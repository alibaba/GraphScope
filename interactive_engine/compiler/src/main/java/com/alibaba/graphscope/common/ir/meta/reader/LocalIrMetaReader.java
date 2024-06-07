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
import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.FileFormatType;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphStatistics;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaInputStream;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * a local file system implementation of {@code IrMetaReader}
 */
public class LocalIrMetaReader implements IrMetaReader {
    private static final Logger logger = LoggerFactory.getLogger(LocalIrMetaReader.class);
    private final Configs configs;

    public LocalIrMetaReader(Configs configs) {
        this.configs = configs;
    }

    @Override
    public IrMeta readMeta() throws IOException {
        String metaPath =
                Objects.requireNonNull(
                        GraphConfig.GRAPH_SCHEMA.get(configs), "ir meta path not exist");
        FileFormatType formatType = FileUtils.getFormatType(metaPath);
        IrGraphSchema graphSchema =
                new IrGraphSchema(new SchemaInputStream(new FileInputStream(metaPath), formatType));
        IrMeta irMeta =
                (formatType == FileFormatType.YAML)
                        ? new IrMeta(
                                graphSchema,
                                new GraphStoredProcedures(new FileInputStream(metaPath), this))
                        : new IrMeta(graphSchema);
        return irMeta;
    }

    @Override
    public IrGraphStatistics readStats(GraphId graphId) throws IOException {
        String statsPath = GraphConfig.GRAPH_STATISTICS.get(configs);
        return statsPath.isEmpty() ? null : new IrGraphStatistics(new FileInputStream(statsPath));
    }
}
