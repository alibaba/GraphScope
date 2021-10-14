/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.structure;

import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.graph.MaxGraph;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AbstractElement {

    public final ElementId id;
    public final String label;
    private Map<String, Object> properties;
    protected MaxGraph graph;

    public AbstractElement(
            ElementId id, String label, Map<String, Object> properties, final MaxGraph graph) {
        this.id = id;
        this.label = label;
        this.properties = properties;
        this.graph = graph;
    }

    public Map<String, Object> getProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    public void setProperties(final Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> selectProperties(String... keys) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }
        return Arrays.asList(keys).stream()
                .filter(k -> properties.containsKey(k))
                .map(k -> Pair.of(k, properties.get(k)))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    public void addProperty(String key, Object value) {
        this.properties.put(key, value);
    }

    public void setGraph(MaxGraph graph) {
        this.graph = graph;
    }

    public MaxGraph getBaseGraph() {
        return this.graph;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractElement that = (AbstractElement) o;
        return this.id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label, properties, graph);
    }
}
