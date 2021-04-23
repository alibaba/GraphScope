package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;

import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalQueryPlan;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class ProcessorCaseWhenFunction extends ProcessorFunction {
    private LogicalQueryPlan casePlan;
    private List<Pair<List<LogicalCompare>, LogicalQueryPlan>> whenThenPlanList;
    private LogicalQueryPlan elseEndPlan;

    public ProcessorCaseWhenFunction(LogicalQueryPlan casePlan,
                                     List<Pair<List<LogicalCompare>, LogicalQueryPlan>> whenThenPlanList,
                                     LogicalQueryPlan elseEndPlan) {
        super(OperatorType.JOIN_CASE_WHEN);
        this.casePlan = casePlan;
        this.whenThenPlanList = whenThenPlanList;
        this.elseEndPlan = elseEndPlan;
    }

    public LogicalQueryPlan getCasePlan() {
        return this.casePlan;
    }

    public List<Pair<List<LogicalCompare>, LogicalQueryPlan>> getWhenThenPlanList() {
        return this.whenThenPlanList;
    }

    public LogicalQueryPlan getElseEndPlan() {
        return this.elseEndPlan;
    }
}
