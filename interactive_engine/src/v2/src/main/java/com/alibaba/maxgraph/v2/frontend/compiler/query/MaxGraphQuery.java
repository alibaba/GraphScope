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
package com.alibaba.maxgraph.v2.frontend.compiler.query;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MaxGraphQuery {
    private final String queryId;
    private final SnapshotMaxGraph snapshotMaxGraph;
    private final GraphSchema graphSchema;
    private final QueryFlowManager queryFlowManager;
    private final MaxGraphResultProcessor resultProcessor;

    public MaxGraphQuery(String queryId,
                         SnapshotMaxGraph snapshotMaxGraph,
                         GraphSchema graphSchema,
                         QueryFlowManager queryFlowManager,
                         MaxGraphResultProcessor resultProcessor) {
        this.queryId = queryId;
        this.snapshotMaxGraph = snapshotMaxGraph;
        this.graphSchema = graphSchema;
        this.queryFlowManager = queryFlowManager;
        this.resultProcessor = resultProcessor;
    }

    public MaxGraphResultProcessor getResultProcessor() {
        return checkNotNull(resultProcessor);
    }

    public QueryFlowManager getQueryFlowManager() {
        return checkNotNull(queryFlowManager);
    }

    public Map<Integer, String> getLabelIdNameList() {
        return queryFlowManager.getTreeNodeLabelManager().getUserIndexLabelList();
    }

    public String getQueryId() {
        return this.queryId;
    }

    public GraphSchema getSchema() {
        return this.graphSchema;
    }

    public SnapshotMaxGraph getSnapshotMaxGraph() {
        return this.snapshotMaxGraph;
    }
}
