/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.schema;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;

public class JsonFileSchemaFetcher implements SchemaFetcher {
    private long snapshot;
    private GraphSchema graphSchema;

    public JsonFileSchemaFetcher(String filePath) {
        try {
            String schemaString = IOUtils.toString(new FileInputStream(filePath), "utf-8");
            this.graphSchema = DefaultGraphSchema.buildSchemaFromJson(schemaString);
            this.snapshot = Integer.MAX_VALUE - 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<GraphSchema, Long> getSchemaSnapshotPair() {
        return Pair.of(graphSchema, snapshot);
    }

    @Override
    public int getPartitionNum() {
        return -1;
    }

    @Override
    public int getVersion() {
        return -1;
    }
}
