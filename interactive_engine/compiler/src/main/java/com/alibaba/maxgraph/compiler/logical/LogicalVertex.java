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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.DataStatistics;
import com.alibaba.maxgraph.compiler.optimizer.costs.EstimateProvider;
import com.google.common.collect.Lists;

import java.util.List;

public abstract class LogicalVertex implements EstimateProvider {
    protected ProcessorFunction processorFunction;
    private int id;
    private List<QueryFlowOuterClass.RequirementValue.Builder> afterRequirementList;
    private List<QueryFlowOuterClass.RequirementValue.Builder> beforeRequirementList;

    /**
     * If ture, it can access edge and property locally after this operator executed
     */
    private boolean propLocalFlag = false;

    protected double estimatedNumRecords;
    protected double estimatedValueSize;
    /**
     * If true, the vertex is store and should be broadcast
     */
    private boolean storeFlag = false;

    /**
     * Early stop related field
     */
    private QueryFlowOuterClass.EarlyStopArgument.Builder earlyStopFlag = null;

    public LogicalVertex() {
    }

    public LogicalVertex(int id, ProcessorFunction processorFunction, boolean propLocalFlag) {
        this.id = id;
        this.processorFunction = processorFunction;
        this.afterRequirementList = Lists.newArrayList();
        this.beforeRequirementList = Lists.newArrayList();
        this.propLocalFlag = propLocalFlag;
    }

    public ProcessorFunction getProcessorFunction() {
        return processorFunction;
    }

    public int getId() {
        return id;
    }

    public List<QueryFlowOuterClass.RequirementValue.Builder> getAfterRequirementList() {
        return afterRequirementList;
    }

    public List<QueryFlowOuterClass.RequirementValue.Builder> getBeforeRequirementList() {
        return beforeRequirementList;
    }

    public void enablePropLocalFlag() {
        this.propLocalFlag = true;
    }

    public boolean isPropLocalFlag() {
        return this.propLocalFlag;
    }

    public abstract void resetInputVertex(LogicalVertex oldInput, LogicalVertex newInput);

    /**
     * Causes this node to compute its output estimates (such as number of rows, size in bytes)
     * based on the inputs and the compiler hints. The compiler hints are instantiated with conservative
     * default values which are used if no other values are provided. Nodes may access the statistics to
     * determine relevant information.
     *
     * @param statistics The statistics object which may be accessed to get statistical information.
     *                   The parameter may be null, if no statistics are available.
     * @param schema     The graph schema
     */
    public abstract void computeOutputEstimates(DataStatistics statistics, GraphSchema schema);

    public int getStartLabelCount() {
        return (int) afterRequirementList.stream().filter(v -> v.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START).count();
    }

    @Override
    public double getEstimatedOutputSize() {
        return getEstimatedNumRecords() * (getEstimatedAvgWidthPerOutputValue() + getEstimatedAvgWidthOutputLabel() + getEstimatedAvgWidthOutputPath());
    }

    @Override
    public double getEstimatedNumRecords() {
        return estimatedNumRecords;
    }

    @Override
    public double getEstimatedAvgWidthPerOutputValue() {
        return estimatedValueSize;
    }

    @Override
    public double getEstimatedAvgWidthOutputLabel() {
        int startLabelCount = getStartLabelCount();
        return startLabelCount * getEstimatedAvgWidthPerOutputValue();
    }

    @Override
    public double getEstimatedAvgWidthOutputPath() {
        return 0;
    }

    public void enableStoreFlag() {
        this.storeFlag = true;
    }

    public boolean isStoreFlag() {
        return this.storeFlag;
    }

    public void setEarlyStopFlag(QueryFlowOuterClass.EarlyStopArgument.Builder earlyStopFlag) {
        this.earlyStopFlag = earlyStopFlag;
    }

    public QueryFlowOuterClass.EarlyStopArgument.Builder getEarlyStopArgument() {
        return this.earlyStopFlag;
    }

    @Override
    public String toString() {
        return this.processorFunction.getOperatorType().toString() + this.getId();
    }
}
