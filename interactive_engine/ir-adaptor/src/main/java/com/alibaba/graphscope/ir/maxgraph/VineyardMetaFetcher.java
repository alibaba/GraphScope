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

package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.util.IrSchemaParser;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;

import java.util.*;

public class VineyardMetaFetcher implements IrMetaFetcher {
    private IrSchemaParser parser;
    private IrMeta irMeta;

    public VineyardMetaFetcher(InstanceConfig instanceConfig) {
        this.parser = IrSchemaParser.getInstance();
        String schemaPath = instanceConfig.getVineyardSchemaPath();
        JsonFileSchemaFetcher fetcher = new JsonFileSchemaFetcher(schemaPath);
        GraphSchema graphSchema = fetcher.getSchemaSnapshotPair().getLeft();
        this.irMeta = new IrMeta(this.parser.parse(graphSchema));
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(irMeta);
    }
}
