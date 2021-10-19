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
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class LogicalChainUnaryVertex extends LogicalUnaryVertex {
    private List<LogicalVertex> unaryVertexList = Lists.newArrayList();

    public LogicalChainUnaryVertex(int id, LogicalVertex inputVertex) {
        super(id, new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNARY_CHAIN), inputVertex);
    }

    public void addUnaryVertex(LogicalVertex vertex) {
        this.unaryVertexList.add(vertex);
    }

    @Override
    public ProcessorFunction getProcessorFunction() {
        return new ProcessorUnaryChainFunction(this.unaryVertexList);

    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        unaryVertexList.forEach(v -> stringBuilder.append("_").append(v.toString()));
        stringBuilder.append("_").append(this.getId());

        return StringUtils.removeStart(stringBuilder.toString(), "_");
    }
}
