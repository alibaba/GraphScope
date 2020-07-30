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
package com.alibaba.maxgraph.compiler.logical;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorLabelValueFunction;

public class LogicalLabelValueVertex extends LogicalUnaryVertex {
    private LogicalVertex labelValueVertex;
    private int labelId;

    public LogicalLabelValueVertex(int id,
                                   LogicalVertex labelValueVertex,
                                   int labelId,
                                   LogicalVertex inputVertex) {
        super(id, new ProcessorFunction(QueryFlowOuterClass.OperatorType.LABEL_VALUE), inputVertex);
        this.labelValueVertex = labelValueVertex;
        this.labelId = labelId;
    }

    @Override
    public ProcessorFunction getProcessorFunction() {
        return new ProcessorLabelValueFunction(this.labelId, this.labelValueVertex);
    }
}
