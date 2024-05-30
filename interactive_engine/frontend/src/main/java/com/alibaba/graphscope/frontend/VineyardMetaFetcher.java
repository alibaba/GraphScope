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

import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.util.IrSchemaParser;

import java.io.IOException;
import java.util.Optional;

public class VineyardMetaFetcher implements IrMetaFetcher {
    private final IrMeta irMeta;

    public VineyardMetaFetcher(String schemaPath) {
        IrSchemaParser parser = IrSchemaParser.getInstance();
        JsonFileSchemaFetcher fetcher = new JsonFileSchemaFetcher(schemaPath);
        GraphSchema graphSchema = fetcher.getSchemaSnapshotPair().values().iterator().next();
        try {
            // TODO: support statistics, otherwise, the CBO will not work
            this.irMeta = new IrMeta(new IrGraphSchema(graphSchema, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(irMeta);
    }
}
