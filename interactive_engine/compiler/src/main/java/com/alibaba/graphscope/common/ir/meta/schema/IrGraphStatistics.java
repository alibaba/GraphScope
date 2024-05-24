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
import com.alibaba.graphscope.groot.common.schema.api.*;

import java.nio.charset.StandardCharsets;

/**
 * Maintain Graph statistics meta for IR
 */
public class IrGraphStatistics implements GraphStatistics {
    private final GraphStatistics graphStatistics;

    public IrGraphStatistics(MetaDataReader dataReader) throws Exception {
        SchemaInputStream schemaInputStream = dataReader.getStatistics();
        String content =
                new String(
                        schemaInputStream.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        schemaInputStream.getInputStream().close();
        this.graphStatistics = Utils.buildStatisticsFromJson(content);
    }

    public IrGraphStatistics(GraphStatistics statistics) {
        this.graphStatistics = statistics;
    }

    @Override
    public int getVersion() {
        return this.graphStatistics.getVersion();
    }

    @Override
    public Integer getVertexTypeCount(int vertexTypeId) {
        return this.graphStatistics.getVertexTypeCount(vertexTypeId);
    }

    @Override
    public Integer getEdgeTypeCount(int edgeTypeId, int sourceTypeId, int targetTypeId) {
        return this.graphStatistics.getEdgeTypeCount(edgeTypeId, sourceTypeId, targetTypeId);
    }

    @Override
    public Integer getVertexCount() {
        return this.graphStatistics.getVertexCount();
    }

    @Override
    public Integer getEdgeCount() {
        return this.graphStatistics.getEdgeCount();
    }
}
