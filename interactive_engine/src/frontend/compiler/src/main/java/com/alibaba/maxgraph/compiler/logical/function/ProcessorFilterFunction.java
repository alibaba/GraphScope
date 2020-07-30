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
import com.google.common.collect.Lists;

import java.util.List;

public class ProcessorFilterFunction extends ProcessorFunction implements CompareFunction {
    private List<Message.LogicalCompare> logicalCompareList = Lists.newArrayList();

    public ProcessorFilterFunction(QueryFlowOuterClass.OperatorType operatorType, Message.Value.Builder argumentBuilder) {
        super(operatorType, argumentBuilder, null);
    }

    public ProcessorFilterFunction(QueryFlowOuterClass.OperatorType operatorType) {
        this(operatorType, null);
    }

    @Override
    public List<Message.LogicalCompare> getLogicalCompareList() {
        return logicalCompareList;
    }
}
