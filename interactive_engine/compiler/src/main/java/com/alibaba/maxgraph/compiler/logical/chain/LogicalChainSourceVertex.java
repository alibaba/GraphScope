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
package com.alibaba.maxgraph.compiler.logical.chain;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.DataStatistics;
import com.google.common.collect.Lists;

import java.util.List;

public class LogicalChainSourceVertex extends LogicalVertex implements LogicalChainVertex {
    private LogicalSourceVertex logicalSourceVertex;
    private List<LogicalVertex> vertexList = Lists.newArrayList();

    public LogicalChainSourceVertex(LogicalSourceVertex logicalSourceVertex) {
        this.logicalSourceVertex = logicalSourceVertex;
    }

    public LogicalSourceVertex getLogicalSourceVertex() {
        return logicalSourceVertex;
    }

    public void addLogicalVertex(LogicalVertex logicalVertex) {
        this.vertexList.add(logicalVertex);
    }

    @Override
    public ProcessorFunction getProcessorFunction() {
        List<LogicalVertex> logicalVertexList = Lists.newArrayList();
        logicalVertexList.addAll(vertexList);
        return new ProcessorChainSourceFunction(logicalSourceVertex, logicalVertexList);
    }

    @Override
    public int getId() {
        return logicalSourceVertex.getId();
    }

    @Override
    public void resetInputVertex(LogicalVertex oldInput, LogicalVertex newInput) {
        throw new IllegalArgumentException("No input vertex for source");
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics, GraphSchema schema) {
        logicalSourceVertex.computeOutputEstimates(statistics, schema);
        for (LogicalVertex logicalVertex : vertexList) {
            logicalVertex.computeOutputEstimates(statistics, schema);
        }

        LogicalVertex lastVertex = vertexList.get(vertexList.size() - 1);
        this.estimatedNumRecords = lastVertex.getEstimatedNumRecords();
        this.estimatedValueSize = lastVertex.getEstimatedOutputSize();
    }

    @Override
    public int getLastId() {
        LogicalVertex lastVertex = vertexList.get(vertexList.size() - 1);
        return lastVertex.getId();
    }

    @Override
    public String toString() {
        StringBuilder oplist = new StringBuilder(this.logicalSourceVertex.toString());
        for (LogicalVertex vertex : vertexList) {
            oplist.append("_").append(vertex.toString());
        }
        oplist.append("_").append(this.getId());

        return oplist.toString();
    }
}
