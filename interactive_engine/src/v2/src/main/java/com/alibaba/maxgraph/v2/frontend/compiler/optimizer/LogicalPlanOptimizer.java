package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalPlanBuilder;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeBuilder;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeManager;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class LogicalPlanOptimizer {
    private final GraphSchema schema;
    private final long snapshotId;

    public LogicalPlanOptimizer(GraphSchema schema,
                                long snapshotId) {
        this.schema = schema;
        this.snapshotId = snapshotId;
    }

    public QueryFlowManager build(GraphTraversal traversal) {
        TreeBuilder treeBuilder = TreeBuilder.newTreeBuilder(schema);
        TreeManager treeManager = treeBuilder.build(traversal);
        treeManager.optimizeTree();

        LogicalQueryPlan logicalQueryPlan = LogicalPlanBuilder.newBuilder().build(treeManager);
        logicalQueryPlan = logicalQueryPlan.chainOptimize();

        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
        return queryFlowBuilder.prepareQueryFlow(logicalQueryPlan, snapshotId);
    }
}
