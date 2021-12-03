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
package com.alibaba.maxgraph.compiler.logical.function;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;

public class ProcessorLabelValueFunction extends ProcessorFunction {
    private int labelId;
    private LogicalVertex labelValueVertex;
    private boolean requireLabelFlag = false;

    public ProcessorLabelValueFunction(int labelId, LogicalVertex labelValueVertex) {
        super(QueryFlowOuterClass.OperatorType.LABEL_VALUE, Message.Value.newBuilder().setIntValue(labelId));
        this.labelValueVertex = labelValueVertex;
        this.labelId = labelId;
    }

    public void setRequireLabelFlag(boolean requireLabelFlag) {
        this.requireLabelFlag = requireLabelFlag;
    }

    public LogicalVertex getLabelValueVertex() {
        return labelValueVertex;
    }

    public int getLabelId() {
        return this.labelId;
    }

    public boolean getRequireLabelFlag() {
        return this.requireLabelFlag;
    }
}
