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
package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.util.Map;

/**
 * The vertex result in maxgraph
 */
public class VertexResult implements QueryResult {
    private long id;
    private int labelId;
    private String label;
    private Map<String, Object> properties;

    public VertexResult() {

    }

    public VertexResult(long id, int labelId, String label) {
        this(id, labelId, label, Maps.newHashMap());
    }

    public VertexResult(long id, int labelId, String label, Map<String, Object> properties) {
        this.id = id;
        this.labelId = labelId;
        this.label = label;
        this.properties = properties;
    }

    public void addProperty(String key, Object value) {
        this.properties.put(key, value);
    }

    public void addProperties(Map<String, Object> properties) {
        this.properties.putAll(properties);
    }

    public long getId() {
        return id;
    }

    public int getLabelId() {
        return labelId;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public Object convertToGremlinStructure() {
        DetachedVertex.Builder builder = DetachedVertex.build()
                .setId(new CompositeId(this.id, this.labelId))
                .setLabel(this.label);
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            DetachedVertexProperty vertexProperty = DetachedVertexProperty.build()
                    .setId(prop.getValue())
                    .setLabel(prop.getKey())
                    .setValue(prop.getValue())
                    .create();
            builder.addProperty(vertexProperty);
        }

        return builder.create();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id)
                .add("label", label)
                .add("propertyList", properties).toString();
    }
}
