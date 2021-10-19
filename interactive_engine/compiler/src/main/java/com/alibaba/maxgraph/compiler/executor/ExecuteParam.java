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
package com.alibaba.maxgraph.compiler.executor;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;

import java.util.List;

public class ExecuteParam {
    private Message.Value value;
    private List<Message.LogicalCompare> logicalCompareList;
    private List<QueryFlowOuterClass.OperatorBase> chainedFunctionList;

    private ExecuteParam(
            Message.Value value,
            List<Message.LogicalCompare> logicalCompareList,
            List<QueryFlowOuterClass.OperatorBase> chainedFunctionList) {
        this.value = value;
        this.logicalCompareList = logicalCompareList;
        this.chainedFunctionList = chainedFunctionList;
    }

    public boolean hasValueParam() {
        return value != null;
    }

    public boolean hasLogicalParam() {
        return logicalCompareList != null && !logicalCompareList.isEmpty();
    }

    public boolean isEmptyParam() {
        return null == value && null == logicalCompareList && null == chainedFunctionList;
    }

    public Message.Value getValue() {
        return value;
    }

    public List<Message.LogicalCompare> getLogicalCompareList() {
        return logicalCompareList;
    }

    public List<QueryFlowOuterClass.OperatorBase> getChainedFunctionList() {
        return chainedFunctionList;
    }

    public static ExecuteParam emptyParam() {
        return new ExecuteParam(null, null, null);
    }

    public static ExecuteParam valueOf(Message.Value value, List<Message.LogicalCompare> logicalCompareList) {
        return new ExecuteParam(value, logicalCompareList, null);
    }

    public static ExecuteParam compareOf(List<Message.LogicalCompare> logicalCompareList) {
        return new ExecuteParam(null, logicalCompareList, null);
    }

    public static ExecuteParam functionOf(List<QueryFlowOuterClass.OperatorBase> chainedFunctionList) {
        return new ExecuteParam(null, null, chainedFunctionList);
    }

    public boolean hasFunctionParam() {
        return chainedFunctionList != null && !chainedFunctionList.isEmpty();
    }
}
