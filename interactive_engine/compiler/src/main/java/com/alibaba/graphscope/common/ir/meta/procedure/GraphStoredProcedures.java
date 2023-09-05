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

package com.alibaba.graphscope.common.ir.meta.procedure;

import com.alibaba.graphscope.common.ir.meta.reader.MetaDataReader;
import com.google.common.collect.Maps;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.InputStream;
import java.util.Map;

public class GraphStoredProcedures implements StoredProcedures {
    private final Map<String, StoredProcedureMeta> storedProcedureMetaMap;

    public GraphStoredProcedures(MetaDataReader reader) throws Exception {
        this.storedProcedureMetaMap = Maps.newLinkedHashMap();
        for (InputStream inputStream : reader.getStoredProcedures()) {
            StoredProcedureMeta createdMeta = StoredProcedureMeta.Deserializer.perform(inputStream);
            this.storedProcedureMetaMap.put(createdMeta.getName(), createdMeta);
            inputStream.close();
        }
    }

    @Override
    public @Nullable StoredProcedureMeta getStoredProcedure(String procedureName) {
        return this.storedProcedureMetaMap.get(procedureName);
    }
}
