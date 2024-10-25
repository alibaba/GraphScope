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

package com.alibaba.graphscope.frontend;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.groot.common.exception.UnimplementedException;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphSchema;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class VineyardIrMetaReader implements IrMetaReader {
    private final Configs configs;

    public VineyardIrMetaReader(Configs configs) {
        this.configs = configs;
    }

    @Override
    public IrMeta readMeta() throws IOException {
        String schemaString =
                FileUtils.readFileToString(
                        new File(GraphConfig.GRAPH_META_SCHEMA_URI.get(configs)),
                        StandardCharsets.UTF_8);
        GraphSchema graphSchema = DefaultGraphSchema.buildSchemaFromJson(schemaString);
        return new IrMeta(new IrGraphSchema(graphSchema, true));
    }

    @Override
    public GraphStatistics readStats(GraphId graphId) {
        // TODO: support statistics, otherwise, the CBO will not work
        throw new UnimplementedException(
                "reading graph statistics in vineyard is unimplemented yet");
    }

    @Override
    public boolean syncStatsEnabled(GraphId graphId) throws IOException {
        return false;
    }
}
