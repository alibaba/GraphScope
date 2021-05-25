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
package com.alibaba.maxgraph.v2.frontend.compiler.logical;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.RequirementType;
import com.alibaba.maxgraph.proto.v2.RequirementValue;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.DataStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class LogicalBinaryVertex extends LogicalVertex {
    private LogicalVertex leftInput;
    private LogicalVertex rightInput;

    public LogicalBinaryVertex(int id, ProcessorFunction processorFunction, boolean propLocalFlag, LogicalVertex leftInput, LogicalVertex rightInput) {
        super(id, processorFunction, propLocalFlag);
        this.leftInput = leftInput;
        this.rightInput = rightInput;

        OperatorType operatorType = processorFunction.getOperatorType();
        if (operatorType != OperatorType.UNION
                && operatorType != OperatorType.DFS_FINISH_JOIN
                && operatorType != OperatorType.JOIN_STORE_FILTER) {
            this.getAfterRequirementList()
                    .add(RequirementValue.newBuilder().setReqType(RequirementType.KEY_DEL));
        }
    }

    public LogicalVertex getLeftInput() {
        return leftInput;
    }

    public LogicalVertex getRightInput() {
        return rightInput;
    }

    @Override
    public void resetInputVertex(LogicalVertex oldInput, LogicalVertex newInput) {
        if (this.leftInput == oldInput) {
            this.leftInput = newInput;
        }
        if (this.rightInput == oldInput) {
            this.rightInput = newInput;
        }
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics, GraphSchema schema) {
        this.estimatedNumRecords = leftInput.getEstimatedNumRecords();
        this.estimatedValueSize = leftInput.getEstimatedOutputSize();
    }

    public void removeAfterKeyDelete() {
        List<RequirementValue.Builder> builderList = this.getAfterRequirementList()
                .stream()
                .filter(v -> v.getReqType() != RequirementType.KEY_DEL)
                .collect(Collectors.toList());
        this.getAfterRequirementList().clear();
        this.getAfterRequirementList().addAll(builderList);

    }

    public boolean containsAfterKeyDelete() {
        for (RequirementValue.Builder builder : this.getAfterRequirementList()) {
            if (builder.getReqType() == RequirementType.KEY_DEL) {
                return true;
            }
        }
        return false;
    }
}
