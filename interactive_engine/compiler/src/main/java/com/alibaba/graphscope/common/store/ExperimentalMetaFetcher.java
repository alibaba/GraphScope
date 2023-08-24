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

package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.procedure.reader.StoredProceduresReader;
import com.alibaba.graphscope.common.ir.schema.FileFormatType;
import com.alibaba.graphscope.common.ir.schema.IrGraphSchema;

import java.io.FileInputStream;
import java.util.Optional;

public class ExperimentalMetaFetcher implements IrMetaFetcher {
    private final IrMeta meta;

    public ExperimentalMetaFetcher(Configs configs) throws Exception {
        String schemaFile = GraphConfig.GRAPH_SCHEMA.get(configs);
        this.meta =
                new IrMeta(
                        new IrGraphSchema(
                                new FileInputStream(schemaFile), getFormatType(schemaFile)),
                        new GraphStoredProcedures(StoredProceduresReader.Factory.create(configs)));
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(this.meta);
    }

    private FileFormatType getFormatType(String schemaFile) {
        if (schemaFile.endsWith(".yaml")) {
            return FileFormatType.YAML;
        } else if (schemaFile.endsWith(".json")) {
            return FileFormatType.JSON;
        } else {
            throw new IllegalArgumentException("unsupported file format " + schemaFile);
        }
    }
}
