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
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;

public class ProcessorRepeatFunction extends ProcessorFunction {
    private LogicalQueryPlan repeatPlan;
    private LogicalVertex enterVertex;
    private LogicalVertex leaveVertex;
    private LogicalVertex feedbackVertex;
    private long maxLoopTimes;
    private boolean hasEmitFlag = false;

    public ProcessorRepeatFunction() {
        this(null, null);
    }

    public ProcessorRepeatFunction(QueryFlowOuterClass.RangeLimit.Builder rangeLimit) {
        this(null, rangeLimit);
    }

    public ProcessorRepeatFunction(Message.Value.Builder argumentBuilder) {
        this(argumentBuilder, null);
    }

    public ProcessorRepeatFunction(Message.Value.Builder argumentBuilder, QueryFlowOuterClass.RangeLimit.Builder rangeLimit) {
        super(QueryFlowOuterClass.OperatorType.REPEAT, argumentBuilder, rangeLimit);
    }

    public LogicalQueryPlan getRepeatPlan() {
        return repeatPlan;
    }

    public void setRepeatPlan(LogicalQueryPlan repeatPlan) {
        this.repeatPlan = repeatPlan;
    }

    public LogicalVertex getEnterVertex() {
        return enterVertex;
    }

    public void setEnterVertex(LogicalVertex enterVertex) {
        this.enterVertex = enterVertex;
    }

    public LogicalVertex getLeaveVertex() {
        return leaveVertex;
    }

    public void setLeaveVertex(LogicalVertex leaveVertex) {
        this.leaveVertex = leaveVertex;
    }

    public LogicalVertex getFeedbackVertex() {
        return feedbackVertex;
    }

    public void setFeedbackVertex(LogicalVertex feedbackVertex) {
        this.feedbackVertex = feedbackVertex;
    }

    public long getMaxLoopTimes() {
        return maxLoopTimes;
    }

    public void setMaxLoopTimes(long maxLoopTimes) {
        this.maxLoopTimes = maxLoopTimes;
    }

    public void enableEmitFlag() {
        this.hasEmitFlag = true;
    }

    public boolean isHasEmitFlag() {
        return this.hasEmitFlag;
    }
}
