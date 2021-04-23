package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.RangeLimit;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;

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

    public ProcessorRepeatFunction(RangeLimit.Builder rangeLimit) {
        this(null, rangeLimit);
    }

    public ProcessorRepeatFunction(Value.Builder argumentBuilder) {
        this(argumentBuilder, null);
    }

    public ProcessorRepeatFunction(Value.Builder argumentBuilder, RangeLimit.Builder rangeLimit) {
        super(OperatorType.REPEAT, argumentBuilder, rangeLimit);
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
