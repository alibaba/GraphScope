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

package com.alibaba.graphscope.groot.servers.ir;

import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.common.util.IrSchemaParser;
import com.alibaba.graphscope.compiler.api.schema.*;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class GrootMetaFetcher implements IrMetaFetcher {
    private IrSchemaParser parser;
    private SchemaFetcher schemaFetcher;

    public GrootMetaFetcher(SchemaFetcher schemaFetcher) {
        this.parser = IrSchemaParser.getInstance();
        this.schemaFetcher = schemaFetcher;
    }

    @Override
    public Optional<IrMeta> fetch() {
        Pair<GraphSchema, Long> pair = this.schemaFetcher.getSchemaSnapshotPair();
        GraphSchema schema;
        Long snapshotId;
        if (pair != null
                && (schema = pair.getLeft()) != null
                && (snapshotId = pair.getRight()) != null) {
            return Optional.of(new IrMeta(parser.parse(schema), snapshotId));
        } else {
            return Optional.empty();
        }
    }
}
