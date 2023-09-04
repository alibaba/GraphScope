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

import com.alibaba.graphscope.common.ir.meta.reader.MetaDataReader;
import com.alibaba.graphscope.common.ir.meta.reader.SchemaInputStream;
import com.alibaba.graphscope.groot.common.exception.GraphElementNotFoundException;
import com.alibaba.graphscope.groot.common.exception.GraphPropertyNotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.util.IrSchemaParser;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Maintain Graph schema meta for IR and add two extra interfaces : {@link #schemaJson()} and {@link #isColumnId()}
 */
public class IrGraphSchema implements GraphSchema {
    private final GraphSchema graphSchema;
    private final String schemeJson;
    private final boolean isColumnId;

    public IrGraphSchema(MetaDataReader dataReader) throws Exception {
        SchemaInputStream schemaInputStream = dataReader.getGraphSchema();
        String content =
                new String(
                        schemaInputStream.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        schemaInputStream.getInputStream().close();
        switch (schemaInputStream.getFormatType()) {
            case YAML:
                this.graphSchema = Utils.buildSchemaFromYaml(content);
                this.schemeJson = IrSchemaParser.getInstance().parse(this.graphSchema);
                break;
            case JSON:
            default:
                this.graphSchema = Utils.buildSchemaFromJson(content);
                this.schemeJson = content;
        }
        this.isColumnId = false;
    }

    public IrGraphSchema(GraphSchema graphSchema, boolean isColumnId) {
        this.graphSchema = graphSchema;
        this.schemeJson = IrSchemaParser.getInstance().parse(graphSchema);
        this.isColumnId = isColumnId;
    }

    public boolean isColumnId() {
        return this.isColumnId;
    }

    public String schemaJson() {
        return this.schemeJson;
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
}
