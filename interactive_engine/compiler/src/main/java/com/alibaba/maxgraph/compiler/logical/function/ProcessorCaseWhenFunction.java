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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class ProcessorCaseWhenFunction extends ProcessorFunction {
    private LogicalQueryPlan casePlan;
    private List<Pair<List<Message.LogicalCompare>, LogicalQueryPlan>> whenThenPlanList;
    private LogicalQueryPlan elseEndPlan;

    public ProcessorCaseWhenFunction(LogicalQueryPlan casePlan,
                                     List<Pair<List<Message.LogicalCompare>, LogicalQueryPlan>> whenThenPlanList,
                                     LogicalQueryPlan elseEndPlan) {
        super(QueryFlowOuterClass.OperatorType.JOIN_CASE_WHEN);
        this.casePlan = casePlan;
        this.whenThenPlanList = whenThenPlanList;
        this.elseEndPlan = elseEndPlan;
    }

    public LogicalQueryPlan getCasePlan() {
        return this.casePlan;
    }

    public List<Pair<List<Message.LogicalCompare>, LogicalQueryPlan>> getWhenThenPlanList() {
        return this.whenThenPlanList;
    }

    public LogicalQueryPlan getElseEndPlan() {
        return this.elseEndPlan;
    }
}
