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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;

import java.util.List;

public class ProcessorChainSourceFunction extends ProcessorFunction implements ProcessorChainFunction {
    private LogicalSourceVertex sourceVertex;
    private List<LogicalVertex> logicalVertexList;

    public ProcessorChainSourceFunction(LogicalSourceVertex sourceVertex, List<LogicalVertex> logicalVertexList) {
        super(QueryFlowOuterClass.OperatorType.SOURCE_CHAIN);
        this.sourceVertex = sourceVertex;
        this.logicalVertexList = logicalVertexList;
    }

    @Override
    public List<LogicalVertex> getChainVertexList() {
        return logicalVertexList;
    }
}
