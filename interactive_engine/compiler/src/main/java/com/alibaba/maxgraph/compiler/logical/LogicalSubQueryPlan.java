/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.logical;

import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
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
        List<Pair<LogicalEdge, LogicalVertex>> targetPairList =
                getTargetEdgeVertexList(sourceVertex);
        if (targetPairList.size() != 1) {
            throw new IllegalArgumentException("Target must be 1 for set stream index");
        }
        targetPairList.get(0).getLeft().setStreamIndex(streamIndex);
    }
}
