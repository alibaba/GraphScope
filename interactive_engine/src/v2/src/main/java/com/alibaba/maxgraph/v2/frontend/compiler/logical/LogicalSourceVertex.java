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

import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.DataStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class LogicalSourceVertex extends LogicalVertex {
    private static final long SOURCE_VALUE_SIZE = 12; //int label + long id

    public LogicalSourceVertex(int id, ProcessorSourceFunction processorSourceFunction) {
        this(id, processorSourceFunction, true);
    }

    public LogicalSourceVertex(int id, ProcessorSourceFunction processorSourceFunction, boolean propLocalFlag) {
        super(id, processorSourceFunction, propLocalFlag);
    }

    @Override
    public void resetInputVertex(LogicalVertex oldInput, LogicalVertex newInput) {
        throw new IllegalArgumentException("No input vertex for source");
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics, GraphSchema schema) {
        ProcessorSourceFunction sourceFunction = getSourceFunction();
        Value.Builder argumentBuilder = sourceFunction.getArgumentBuilder();
        long idCount = argumentBuilder.getLongValueListCount();
        switch (sourceFunction.getOperatorType()) {
            case V:
            case E: {
                if (idCount > 0) {
                    estimatedNumRecords = idCount;
                } else {
                    estimatedNumRecords = statistics.getLabelEdgeCount(
                            argumentBuilder
                                    .getIntValueListList()
                                    .stream()
                                    .map(v -> schema.getSchemaElement(v).getLabel())
                                    .collect(Collectors.toList()));
                }
                List<LogicalCompare> logicalCompareList = sourceFunction.getLogicalCompareList();
                estimatedNumRecords *= Math.pow(0.5, logicalCompareList.size());
                break;
            }
            default: {
                throw new IllegalArgumentException(processorFunction.getOperatorType().toString());
            }
        }
        super.estimatedValueSize = SOURCE_VALUE_SIZE;
    }

    private ProcessorSourceFunction getSourceFunction() {
        return ProcessorSourceFunction.class.cast(processorFunction);
    }
}
