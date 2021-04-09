package com.alibaba.maxgraph.v2.frontend.compiler.logical;

import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class LogicalSubQueryPlan extends LogicalQueryPlan {
    private boolean delegateSourceFlag = true;

    public LogicalSubQueryPlan(ContextManager contextManager) {
        super(contextManager);
    }

    public boolean isDelegateSourceFlag() {
        return delegateSourceFlag;
    }

    public void setDelegateSourceFlag(boolean delegateSourceFlag) {
        this.delegateSourceFlag = delegateSourceFlag;
    }

    public void setSourceStreamIndex(int streamIndex) {
        LogicalVertex sourceVertex = getSourceVertex();
        List<Pair<LogicalEdge, LogicalVertex>> targetPairList = getTargetEdgeVertexList(sourceVertex);
        if (targetPairList.size() != 1) {
            throw new IllegalArgumentException("Target must be 1 for set stream index");
        }
        targetPairList.get(0).getLeft().setStreamIndex(streamIndex);
    }
}
