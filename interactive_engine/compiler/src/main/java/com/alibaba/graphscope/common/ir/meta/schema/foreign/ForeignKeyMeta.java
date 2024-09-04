/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema.foreign;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

public class ForeignKeyMeta {
    private final Map<EdgeTypeId, ForeignKeyEntry> keyMap;

    public ForeignKeyMeta(String foreignKeyUri) {
        Preconditions.checkArgument(!foreignKeyUri.isEmpty(), "invalid foreign key uri");
        this.keyMap = createKeyMap(foreignKeyUri);
    }

    private Map<EdgeTypeId, ForeignKeyEntry> createKeyMap(String foreignKeyUri) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<EdgeTypeId, ForeignKeyEntry> keyMap = Maps.newHashMap();
            String foreignKeyJson =
                    FileUtils.readFileToString(new File(foreignKeyUri), StandardCharsets.UTF_8);
            JsonNode jsonNode = mapper.readTree(foreignKeyJson);
            Iterator<JsonNode> iterator = jsonNode.iterator();
            while (iterator.hasNext()) {
                JsonNode entry = iterator.next();
                JsonNode labelNode = entry.get("label");
                EdgeTypeId edgeTypeId =
                        new EdgeTypeId(
                                labelNode.get("src_id").asInt(),
                                labelNode.get("dst_id").asInt(),
                                labelNode.get("id").asInt());
                JsonNode foreignNode = entry.get("foreign_keys");
                ForeignKeyEntry foreignKeyEntry = new ForeignKeyEntry();
                Iterator it0 = foreignNode.iterator();
                while (it0.hasNext()) {
                    JsonNode keyNode = (JsonNode) it0.next();
                    foreignKeyEntry.add(
                            new ForeignKey(keyNode.get("id").asInt(), keyNode.get("key").asText()));
                }
                keyMap.put(edgeTypeId, foreignKeyEntry);
            }
            return keyMap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ForeignKeyEntry getForeignKeyEntry(EdgeTypeId typeId) {
        return this.keyMap.get(typeId);
    }

    @Override
    public String toString() {
        return "ForeignKeyMeta{" + "keyMap=" + keyMap + '}';
    }
}
