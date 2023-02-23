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

package com.alibaba.graphscope.common.ir.schema;

import static com.alibaba.graphscope.common.ir.util.Static.RESOURCE;

import com.alibaba.graphscope.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.graphscope.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.*;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A wrapper class for {@link GraphSchema}
 */
public class GraphSchemaWrapper implements StatisticSchema {
    private final GraphSchema graphSchema;
    private final boolean isColumnId;

    public GraphSchemaWrapper(GraphSchema graphSchema, boolean isColumnId) {
        this.graphSchema = graphSchema;
        this.isColumnId = isColumnId;
    }

    @Override
    public Statistic getStatistic(List<String> tableName) {
        ObjectUtils.requireNonEmpty(tableName);
        String labelName = tableName.get(0);
        try {
            return new DefaultStatistic(this.graphSchema.getElement(labelName));
        } catch (GraphElementNotFoundException e) {
            throw RESOURCE.tableNotFound(labelName).ex();
        }
    }

    @Override
    public boolean isColumnId() {
        return this.isColumnId;
    }

    @Override
    public GraphElement getElement(String s) throws GraphElementNotFoundException {
        return this.graphSchema.getElement(s);
    }

    @Override
    public GraphElement getElement(int i) throws GraphElementNotFoundException {
        return this.graphSchema.getElement(i);
    }

    @Override
    public List<GraphVertex> getVertexList() {
        return this.graphSchema.getVertexList();
    }

    @Override
    public List<GraphEdge> getEdgeList() {
        return this.graphSchema.getEdgeList();
    }

    @Override
    public Integer getPropertyId(String s) throws GraphPropertyNotFoundException {
        return this.graphSchema.getPropertyId(s);
    }

    @Override
    public String getPropertyName(int i) throws GraphPropertyNotFoundException {
        return this.graphSchema.getPropertyName(i);
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(String s) {
        return this.graphSchema.getPropertyList(s);
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(int i) {
        return this.graphSchema.getPropertyList(i);
    }

    @Override
    public int getVersion() {
        return this.getVersion();
    }

    /**
     * An inner class to implement interfaces related to primary keys from {@code Statistic}
     */
    private class DefaultStatistic implements Statistic {
        private GraphElement element;
        private ImmutableBitSet primaryBitSet;

        public DefaultStatistic(GraphElement element) {
            Objects.requireNonNull(element);
            this.element = element;
            List<GraphProperty> primaryKeys = this.element.getPrimaryKeyList();
            if (ObjectUtils.isEmpty(primaryKeys)) {
                this.primaryBitSet = ImmutableBitSet.of();
            } else {
                this.primaryBitSet =
                        ImmutableBitSet.of(
                                primaryKeys.stream()
                                        .map(k -> k.getId())
                                        .collect(Collectors.toList()));
            }
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return this.primaryBitSet.isEmpty() ? false : this.primaryBitSet.equals(columns);
        }

        @Override
        public @Nullable List<ImmutableBitSet> getKeys() {
            return ImmutableList.of(primaryBitSet);
        }
    }
}
