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
package com.alibaba.maxgraph.compiler.optimizer;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;

public class QueryFlowManager {
    private QueryFlowOuterClass.QueryFlow.Builder queryFlow;
    private OperatorListManager operatorListManager;
    private TreeNodeLabelManager treeNodeLabelManager;
    private ValueType resultValueType;

    public QueryFlowManager(QueryFlowOuterClass.QueryFlow.Builder queryFlow,
                            OperatorListManager operatorListManager,
                            TreeNodeLabelManager treeNodeLabelManager,
                            ValueType resultValueType) {
        this.queryFlow = queryFlow;
        this.operatorListManager = operatorListManager;
        this.treeNodeLabelManager = treeNodeLabelManager;
        this.resultValueType = resultValueType;
    }

    public QueryFlowOuterClass.QueryFlow.Builder getQueryFlow() {
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

    public boolean checkValidPrepareFlow() {
        return null != operatorListManager && !operatorListManager.getPrepareEntityList().isEmpty();
    }

    public void validQueryFlow() {
        QueryFlowOuterClass.QueryFlow queryFlow = getQueryFlow().build();
        QueryFlowOuterClass.SourceOperator sourceOperator = queryFlow.getQueryPlan().getSourceOp();
        if (sourceOperator.hasOdpsInput() && sourceOperator.getBase().getArgument().getIntValueListCount() != 1) {
            throw new IllegalArgumentException("There must be one label argument for odps input");
        }
    }
}
