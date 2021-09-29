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
package com.alibaba.graphscope.gaia.store;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class GraphStoreService extends GraphElementId {
    protected Map<String, Map<String, Map<String, Object>>> cachedPropertyForTest;

    public GraphStoreService(String propertyResourceName) {
        try {
            // init properties from file under resources
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyResourceName);
            String propertiesJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            this.cachedPropertyForTest = JsonUtils.fromJson(propertiesJson, new TypeReference<Map<String, Map<String, Map<String, Object>>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract long getLabelId(String label);

    public abstract String getLabel(long labelId);

    public abstract long getGlobalId(long labelId, long propertyId);

    public int getPropertyId(String propertyName) {
        throw new UnsupportedOperationException();
    }

    public String getPropertyName(int propertyId) {
        throw new UnsupportedOperationException();
    }

    public long getSnapShotId() {
        throw new UnsupportedOperationException();
    }

    public void updateSnapShotId() {
        throw new UnsupportedOperationException();
    }

    public <P> Optional<P> getVertexProperty(BigInteger id, String key) {
        String idStr = String.valueOf(id);
        if (getVertexKeys(id).isEmpty()) return Optional.empty();
        return Optional.ofNullable((P) cachedPropertyForTest.get("vertex_properties").get(idStr).get(key));
    }

    public Set<String> getVertexKeys(BigInteger id) {
        String idStr = String.valueOf(id);
        Map<String, Object> result = cachedPropertyForTest.get("vertex_properties").get(idStr);
        if (result == null) return Collections.EMPTY_SET;
        return result.keySet();
    }

    public <P> Optional<P> getEdgeProperty(GremlinResult.Edge edge, String key) {
        Object id = getCompositeId(edge);
        String idStr = String.valueOf(id);
        if (getEdgeKeys(edge).isEmpty()) return Optional.empty();
        return Optional.ofNullable((P) cachedPropertyForTest.get("edge_properties").get(idStr).get(key));
    }

    public Set<String> getEdgeKeys(GremlinResult.Edge edge) {
        Object id = getCompositeId(edge);
        String idStr = String.valueOf(id);
        Map<String, Object> result = cachedPropertyForTest.get("edge_properties").get(idStr);
        if (result == null) return Collections.EMPTY_SET;
        return result.keySet();
    }

    public abstract Object getCompositeId(GremlinResult.Edge edge);
}
