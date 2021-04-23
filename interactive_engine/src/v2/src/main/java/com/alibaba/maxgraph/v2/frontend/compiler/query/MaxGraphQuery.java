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
