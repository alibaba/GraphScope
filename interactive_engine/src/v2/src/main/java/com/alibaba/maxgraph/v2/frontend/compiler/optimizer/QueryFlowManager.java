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
package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

import com.alibaba.maxgraph.proto.v2.QueryFlow;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class QueryFlowManager {
    private QueryFlow.Builder queryFlow;
    private OperatorListManager operatorListManager;
    private TreeNodeLabelManager treeNodeLabelManager;
    private ValueType resultValueType;

    public QueryFlowManager(QueryFlow.Builder queryFlow,
                            OperatorListManager operatorListManager,
                            TreeNodeLabelManager treeNodeLabelManager,
                            ValueType resultValueType) {
        this.queryFlow = queryFlow;
        this.operatorListManager = operatorListManager;
        this.treeNodeLabelManager = treeNodeLabelManager;
        this.resultValueType = resultValueType;
    }

    public QueryFlow.Builder getQueryFlow() {
        return queryFlow;
    }

    public OperatorListManager getOperatorListManager() {
        return operatorListManager;
    }

    public TreeNodeLabelManager getTreeNodeLabelManager() {
        return treeNodeLabelManager;
    }

    public ValueType getResultValueType() {
        return resultValueType;
    }
}
