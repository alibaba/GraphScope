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

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.ir.tools.QueryIdGenerator;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.base.Preconditions;

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

import java.util.List;
import java.util.concurrent.Executor;

public class GraphQueryExecutor extends FabricExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GraphQueryExecutor.class);
    private static final String GET_ROUTING_TABLE_STATEMENT =
            "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)";
    private static String PING_STATEMENT = "CALL db.ping()";
    private final Configs graphConfig;
    private final IrMetaQueryCallback metaQueryCallback;
    private final ExecutionClient client;

    private final QueryIdGenerator idGenerator;
    private final FabricConfig fabricConfig;
    private final QueryCache queryCache;

    public GraphQueryExecutor(
            FabricConfig config,
            FabricPlanner planner,
            UseEvaluation useEvaluation,
            CatalogManager catalogManager,
            LogProvider internalLog,
            FabricStatementLifecycles statementLifecycles,
            Executor fabricWorkerExecutor,
            Configs graphConfig,
            QueryIdGenerator idGenerator,
            IrMetaQueryCallback metaQueryCallback,
            ExecutionClient client,
            QueryCache queryCache) {
        super(
                config,
                planner,
                useEvaluation,
                catalogManager,
                internalLog,
                statementLifecycles,
                fabricWorkerExecutor);
        this.fabricConfig = config;
        this.graphConfig = graphConfig;
        this.idGenerator = idGenerator;
        this.metaQueryCallback = metaQueryCallback;
        this.client = client;
        this.queryCache = queryCache;
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
    public StatementResult run(
            FabricTransaction fabricTransaction, String statement, MapValue parameters) {
        IrMeta irMeta = null;
        try {
            // hack ways to execute routing table or ping statement before executing the real query
            if (statement.equals(GET_ROUTING_TABLE_STATEMENT) || statement.equals(PING_STATEMENT)) {
                return super.run(fabricTransaction, statement, parameters);
            }
            irMeta = metaQueryCallback.beforeExec();
            QueryCache.Key cacheKey = queryCache.createKey(statement, irMeta);
            QueryCache.Value cacheValue = queryCache.get(cacheKey);
            Preconditions.checkArgument(
                    cacheValue != null,
                    "value should have been loaded automatically in query cache");
            long jobId = idGenerator.generateId();
            String jobName = idGenerator.generateName(jobId);
            GraphPlanner.Summary planSummary =
                    new GraphPlanner.Summary(
                            cacheValue.summary.getLogicalPlan(),
                            cacheValue.summary.getPhysicalPlan());
            logger.debug(
                    "cypher query \"{}\", job conf name \"{}\", calcite logical plan {}, hash id"
                            + " {}",
                    statement,
                    jobName,
                    planSummary.getLogicalPlan().explain(),
                    cacheKey.hashCode());
            if (planSummary.getLogicalPlan().isReturnEmpty()) {
                return StatementResults.initial();
            }
            logger.info(
                    "cypher query \"{}\", job conf name \"{}\", ir core logical plan {}",
                    statement,
                    jobName,
                    planSummary.getPhysicalPlan().explain());
            StatementResults.SubscribableExecution execution;
            if (cacheValue.result != null && cacheValue.result.isCompleted) {
                execution =
                        new AbstractPlanExecution(planSummary) {
                            @Override
                            protected void execute(ExecutionResponseListener listener) {
                                List<IrResult.Results> records = cacheValue.result.records;
                                records.forEach(k -> listener.onNext(k.getRecord()));
                                listener.onCompleted();
                            }
                        };
            } else {
                execution =
                        new AbstractPlanExecution(planSummary) {
                            @Override
                            protected void execute(ExecutionResponseListener listener)
                                    throws Exception {
                                ExecutionRequest request =
                                        new ExecutionRequest(
                                                jobId,
                                                jobName,
                                                planSummary.getLogicalPlan(),
                                                planSummary.getPhysicalPlan());
                                QueryTimeoutConfig timeoutConfig = getQueryTimeoutConfig();
                                client.submit(request, listener, timeoutConfig);
                            }
                        };
            }
            return StatementResults.connectVia(execution, new QuerySubject.BasicQuerySubject());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (irMeta != null) {
                metaQueryCallback.afterExec(irMeta);
            }
        }
    }

    private QueryTimeoutConfig getQueryTimeoutConfig() {
        return new QueryTimeoutConfig(fabricConfig.getTransactionTimeout().toMillis());
    }
}
