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

import com.alibaba.maxgraph.common.util.CompilerConstant;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversal;
import com.alibaba.maxgraph.compiler.logical.LogicalPlanBuilder;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.tree.TreeBuilder;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class LogicalPlanOptimizer {
    private final OptimizeConfig optimizeConfig;
    private final boolean globalPullGraphFlag;
    private final GraphSchema schema;
    private final long snapshotId;
    private final boolean lambdaEnableFlag;

    public LogicalPlanOptimizer(OptimizeConfig optimizeConfig,
                                boolean globalPullGraphFlag,
                                GraphSchema schema,
                                long snapshotId) {
        this(optimizeConfig, globalPullGraphFlag, schema, snapshotId, false);
    }

    public LogicalPlanOptimizer(OptimizeConfig optimizeConfig,
                                boolean globalPullGraphFlag,
                                GraphSchema schema,
                                long snapshotId,
                                boolean lambdaEnableFlag) {
        this.optimizeConfig = optimizeConfig;
        this.globalPullGraphFlag = globalPullGraphFlag;
        this.schema = schema;
        this.snapshotId = snapshotId;
        this.lambdaEnableFlag = lambdaEnableFlag;
    }

    public QueryFlowManager build(GraphTraversal traversal) {
        TreeBuilder treeBuilder = TreeBuilder.newTreeBuilder(schema, optimizeConfig, lambdaEnableFlag);
        TreeManager treeManager = treeBuilder.build(traversal);
        if (this.globalPullGraphFlag) {
            treeManager.getQueryConfig().addProperty(CompilerConstant.QUERY_GRAPH_PULL_ENABLE, true);
        }
        treeManager.optimizeTree();

        LogicalQueryPlan logicalQueryPlan = LogicalPlanBuilder.newBuilder().build(treeManager);
        logicalQueryPlan = logicalQueryPlan.chainOptimize();

        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(logicalQueryPlan, snapshotId);
        queryFlowManager.validQueryFlow();

        return queryFlowManager;
    }

    /**
     * Build dfs traversal to query flow
     *
     * @param dfsTraversal The given dfs traversal
     * @return The result query flow
     */
    public QueryFlowManager build(DfsTraversal dfsTraversal) {
        TreeBuilder treeBuilder = TreeBuilder.newTreeBuilder(schema, optimizeConfig, this.lambdaEnableFlag);
        treeBuilder.setDisableBarrierOptimizer(true);
        TreeManager treeManager = dfsTraversal.buildDfsTree(treeBuilder, schema);
        if (this.globalPullGraphFlag) {
            treeManager.getQueryConfig().addProperty(CompilerConstant.QUERY_GRAPH_PULL_ENABLE, true);
        }

        LogicalQueryPlan logicalQueryPlan = LogicalPlanBuilder.newBuilder().build(treeManager);
        logicalQueryPlan = logicalQueryPlan.chainOptimize();

        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(logicalQueryPlan, snapshotId);
        queryFlowManager.validQueryFlow();

        return queryFlowManager;
    }
}
