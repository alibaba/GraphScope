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
