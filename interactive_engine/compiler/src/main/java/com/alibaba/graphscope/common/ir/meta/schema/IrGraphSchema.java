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

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.groot.common.exception.PropertyNotFoundException;
import com.alibaba.graphscope.groot.common.exception.TypeNotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Maintain Graph schema meta for IR and add two extra interfaces : {@link #getSchemaSpec(SchemaSpec.Type)} ()} and {@link #isColumnId()}
 */
public class IrGraphSchema implements GraphSchema {
    private final GraphSchema graphSchema;
    private final boolean isColumnId;
    private final SchemaSpecManager specManager;

    public IrGraphSchema(SchemaInputStream schemaInputStream) throws IOException {
        this.isColumnId = false;
        String content =
                new String(
                        schemaInputStream.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        schemaInputStream.getInputStream().close();
        SchemaSpec spec = new SchemaSpec(schemaInputStream.getType(), content);
        this.graphSchema = spec.convert();
        this.specManager = new SchemaSpecManager(this, spec);
    }

    public IrGraphSchema(GraphSchema graphSchema, boolean isColumnId) {
        this.graphSchema = graphSchema;
        this.isColumnId = isColumnId;
        this.specManager = new SchemaSpecManager(this);
    }

    public boolean isColumnId() {
        return this.isColumnId;
    }

    public String getSchemaSpec(SchemaSpec.Type type) {
        return this.specManager.getSpec(type).getContent();
    }

    @Override
    public GraphElement getElement(String s) throws TypeNotFoundException {
        return this.graphSchema.getElement(s);
    }

    @Override
    public GraphElement getElement(int i) throws TypeNotFoundException {
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
    public Integer getPropertyId(String s) throws PropertyNotFoundException {
        return this.graphSchema.getPropertyId(s);
    }

    @Override
    public String getPropertyName(int i) throws PropertyNotFoundException {
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
    public String getVersion() {
        return this.graphSchema.getVersion();
    }

    protected GraphSchema getGraphSchema() {
        return this.graphSchema;
    }
}
