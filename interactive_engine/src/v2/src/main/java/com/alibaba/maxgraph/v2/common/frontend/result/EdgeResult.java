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

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.Map;

/**
 * Edge result for gremlin query
 */
public class EdgeResult implements QueryResult {
    private long srcVertexId;
    private int srcLabelId;
    private String srcLabel;
    private long dstVertexId;
    private int dstLabelId;
    private String dstLabel;
    private ElementId id;
    private String label;
    private Map<String, Object> properties;

    public EdgeResult() {

    }

    public EdgeResult(long srcVertexId,
                      int srcLabelId,
                      String srcLabel,
                      long dstVertexId,
                      int dstLabelId,
                      String dstLabel,
                      ElementId id,
                      String label) {
        this(srcVertexId, srcLabelId, srcLabel, dstVertexId, dstLabelId, dstLabel, id, label, Maps.newHashMap());
    }

    public EdgeResult(long srcVertexId,
                      int srcLabelId,
                      String srcLabel,
                      long dstVertexId,
                      int dstLabelId,
                      String dstLabel,
                      ElementId id,
                      String label,
                      Map<String, Object> properties) {
        this.srcVertexId = srcVertexId;
        this.srcLabelId = srcLabelId;
        this.srcLabel = srcLabel;
        this.dstVertexId = dstVertexId;
        this.dstLabelId = dstLabelId;
        this.dstLabel = dstLabel;
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    public void addProperty(String key, Object value) {
        if (null == this.properties) {
            this.properties = Maps.newHashMap();
        }
        this.properties.put(key, value);
    }

    public long getSrcVertexId() {
        return srcVertexId;
    }

    public int getSrcLabelId() {
        return srcLabelId;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public long getDstVertexId() {
        return dstVertexId;
    }

    public int getDstLabelId() {
        return dstLabelId;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    public ElementId getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EdgeResult that = (EdgeResult) o;
        return srcVertexId == that.srcVertexId &&
                srcLabelId == that.srcLabelId &&
                dstVertexId == that.dstVertexId &&
                dstLabelId == that.dstLabelId &&
                id == that.id &&
                Objects.equal(srcLabel, that.srcLabel) &&
                Objects.equal(dstLabel, that.dstLabel) &&
                Objects.equal(label, that.label) &&
                Objects.equal(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(srcVertexId, srcLabelId, srcLabel, dstVertexId, dstLabelId, dstLabel, id, label, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("srcVertexId", srcVertexId)
                .add("srcLabelId", srcLabelId)
                .add("srcLabel", srcLabel)
                .add("dstVertexId", dstVertexId)
                .add("dstLabelId", dstLabelId)
                .add("dstLabel", dstLabel)
                .add("id", id)
                .add("label", label)
                .add("properties", properties)
                .toString();
    }

    @Override
    public Object convertToGremlinStructure() {
        DetachedEdge.Builder builder = DetachedEdge.build()
                .setId(this.id)
                .setLabel(this.label)
                .setInV(DetachedVertex.build()
                        .setId(new CompositeId(this.dstVertexId, this.dstLabelId))
                        .setLabel(this.dstLabel)
                        .create())
                .setOutV(DetachedVertex.build()
                        .setId(new CompositeId(this.srcVertexId, this.srcLabelId))
                        .setLabel(this.srcLabel)
                        .create());
        if (null != properties) {
            for (Map.Entry<String, Object> prop : properties.entrySet()) {
                builder.addProperty(new DetachedProperty(prop.getKey(), prop.getValue()));
            }
        }
        return builder.create();
    }
}
