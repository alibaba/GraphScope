/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.executor;

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import org.antlr.v4.runtime.tree.ParseTree;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.executor.FabricStatementLifecycles;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.stream.QuerySubject;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.virtual.MapValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class GraphQueryExecutor extends FabricExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GraphQueryExecutor.class);
    private static final String GET_ROUTING_TABLE_STATEMENT = "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)";
    private static String PING_STATEMENT = "CALL db.ping()";
    private final Configs graphConfig;
    private final Antlr4Parser antlr4Parser;
    private final IrMetaQueryCallback metaQueryCallback;
    private final ExecutionClient client;

    private final GraphPlanner graphPlanner;

    public GraphQueryExecutor(
            FabricConfig config,
            FabricPlanner planner,
            UseEvaluation useEvaluation,
            CatalogManager catalogManager,
            LogProvider internalLog,
            FabricStatementLifecycles statementLifecycles,
            Executor fabricWorkerExecutor,
            Configs graphConfig,
            Antlr4Parser antlr4Parser,
            GraphPlanner graphPlanner,
            IrMetaQueryCallback metaQueryCallback,
            ExecutionClient client) {
        super(
                config,
                planner,
                useEvaluation,
                catalogManager,
                internalLog,
                statementLifecycles,
                fabricWorkerExecutor);
        this.graphConfig = graphConfig;
        this.antlr4Parser = antlr4Parser;
        this.graphPlanner = graphPlanner;
        this.metaQueryCallback = metaQueryCallback;
        this.client = client;
    }

    /**
     * build logical plan from cypher statement
     * build physical plan by JobBuilder (currently by ir_core)
     * execute physical plan
     * implement {@link org.neo4j.fabric.stream.StatementResults.SubscribableExecution} to subscribe result
     * @param fabricTransaction
     * @param statement
     * @param parameters
     * @return
     */
    @Override
    public StatementResult run(FabricTransaction fabricTransaction, String statement, MapValue parameters) {
            IrMeta irMeta = null;
        try {
            // hack ways to execute routing table or ping statement before executing the real query
            if (statement.equals(GET_ROUTING_TABLE_STATEMENT) || statement.equals(PING_STATEMENT)) {
                return super.run(fabricTransaction, statement, parameters);
            }
            irMeta = metaQueryCallback.beforeExec();
            ParseTree parseTree = antlr4Parser.parse(statement);
            GraphPlanner.PlannerInstance instance = graphPlanner.instance(parseTree, irMeta);
            GraphPlanner.Summary planSummary = instance.plan();
            if (planSummary.isReturnEmpty()) {
                return StatementResults.initial();
            }
            QuerySubject querySubject = new QuerySubject.BasicQuerySubject();
            StatementResults.SubscribableExecution execution = new GraphPlanExecution(
                    this.client,
                    planSummary);
            metaQueryCallback.afterExec(irMeta);
            return StatementResults.connectVia(execution, querySubject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (irMeta != null) {
                metaQueryCallback.afterExec(irMeta);
            }
        }
    }
}
