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

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaUpdater;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphStoredProcedures {
    private final Map<String, StoredProcedureMeta> storedProcedureMetaMap;
    private IrMetaUpdater metaUpdater;

    public GraphStoredProcedures(InputStream metaStream) {
        Yaml yaml = new Yaml();
        Map<String, Object> yamlAsMap = yaml.load(metaStream);
        List<Object> procedureList = (List<Object>) yamlAsMap.get("stored_procedures");
        if (ObjectUtils.isEmpty(procedureList)) {
            this.storedProcedureMetaMap = Maps.newLinkedHashMap();
        } else {
            this.storedProcedureMetaMap =
                    procedureList.stream()
                            .map(
                                    k -> {
                                        try {
                                            String procedureYaml = yaml.dump(k);
                                            return StoredProcedureMeta.Deserializer.perform(
                                                    new ByteArrayInputStream(
                                                            procedureYaml.getBytes()));
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                            .collect(Collectors.toMap(StoredProcedureMeta::getName, k -> k));
        }
    }

    public @Nullable StoredProcedureMeta getStoredProcedure(String procedureName) {
        return getStoredProcedure(procedureName, true);
    }

    /**
     *
     * @param procedureName
     * @param update determine whether a remote update request should be sent to the admin service when the procedure is not found in the local cache
     * @return
     */
    private @Nullable StoredProcedureMeta getStoredProcedure(String procedureName, boolean update) {
        StoredProcedureMeta cachedProcedure = this.storedProcedureMetaMap.get(procedureName);
        if (cachedProcedure == null) {
            if (update && this.metaUpdater != null) {
                IrMeta meta = this.metaUpdater.onUpdate();
                if (meta != null && meta.getStoredProcedures() != null) {
                    return meta.getStoredProcedures().getStoredProcedure(procedureName, false);
                }
            }
        }
        return cachedProcedure;
    }

    public void registerIrMetaUpdater(IrMetaUpdater updater) {
        this.metaUpdater = updater;
    }

    @Override
    public String toString() {
        return storedProcedureMetaMap.keySet().toString();
    }
}
