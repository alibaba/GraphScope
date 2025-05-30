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
import com.alibaba.graphscope.common.client.HttpExecutionClient;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.client.write.HttpWriteClient;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.tools.*;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.MetricsCollector;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
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

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.Executor;

public class GraphQueryExecutor extends FabricExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GraphQueryExecutor.class);
    private static final String GET_ROUTING_TABLE_STATEMENT =
            "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)";
    private static final String PING_STATEMENT = "CALL db.ping()";
    private final Configs graphConfig;
    private final IrMetaQueryCallback metaQueryCallback;
    private final ExecutionClient client;

    private final QueryIdGenerator idGenerator;
    private final FabricConfig fabricConfig;
    private final QueryCache queryCache;
    private final GraphPlanner graphPlanner;

    private final HttpWriteClient writeClient;

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
            QueryCache queryCache,
            GraphPlanner graphPlanner) {
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
        this.graphPlanner = graphPlanner;
        if (client instanceof HttpExecutionClient) {
            this.writeClient = new HttpWriteClient(((HttpExecutionClient) client).getSession());
        } else {
            this.writeClient = null;
        }
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
        final BigInteger jobId = idGenerator.generateId();
        final QueryStatusCallback statusCallback =
                ClassUtils.createQueryStatusCallback(
                        jobId,
                        null,
                        statement,
                        new MetricsCollector.Cypher(System.currentTimeMillis()),
                        null,
                        graphConfig);
        try {
            statusCallback
                    .getQueryLogger()
                    .info("[query][received]: query received from the cypher client");
            // hack ways to execute routing table or ping statement before executing the real query
            if (statement.equals(GET_ROUTING_TABLE_STATEMENT) || statement.equals(PING_STATEMENT)) {
                return super.run(fabricTransaction, statement, parameters);
            }
            irMeta = metaQueryCallback.beforeExec();
            QueryCache.Key cacheKey =
                    queryCache.createKey(
                            graphPlanner.instance(
                                    statement, irMeta, statusCallback.getQueryLogger()));
            QueryCache.Value cacheValue = queryCache.get(cacheKey);
            logCacheHit(cacheKey, cacheValue);
            Preconditions.checkArgument(
                    cacheValue != null,
                    "value should have been loaded automatically in query cache");
            String jobName = idGenerator.generateName(jobId);
            GraphPlanner.Summary planSummary =
                    new GraphPlanner.Summary(
                            cacheValue.summary.getLogicalPlan(),
                            cacheValue.summary.getPhysicalPlan());
            statusCallback
                    .getQueryLogger()
                    .info("logical IR plan \n\n {} \n\n", planSummary.getLogicalPlan().explain());
            boolean returnEmpty = planSummary.getLogicalPlan().isReturnEmpty();
            if (!returnEmpty) {
                statusCallback
                        .getQueryLogger()
                        .debug("physical IR plan {}", planSummary.getPhysicalPlan().explain());
            }
            QueryTimeoutConfig timeoutConfig = getQueryTimeoutConfig();
            GraphPlanExecutor executor;
            if (returnEmpty) {
                executor =
                        new GraphPlanExecutor() {
                            @Override
                            public void execute(
                                    GraphPlanner.Summary summary,
                                    IrMeta irMeta,
                                    ExecutionResponseListener listener)
                                    throws Exception {
                                listener.onCompleted();
                            }
                        };
            } else if (cacheValue.result != null && cacheValue.result.isCompleted) {
                executor =
                        new GraphPlanExecutor() {
                            @Override
                            public void execute(
                                    GraphPlanner.Summary summary,
                                    IrMeta irMeta,
                                    ExecutionResponseListener listener)
                                    throws Exception {
                                List<IrResult.Results> records = cacheValue.result.records;
                                records.forEach(k -> listener.onNext(k.getRecord()));
                                listener.onCompleted();
                            }
                        };
            } else if (planSummary.getLogicalPlan().getMode() == LogicalPlan.Mode.SCHEMA) {
                executor = StoredProcedureMeta.Mode.SCHEMA;
            } else if (planSummary.getLogicalPlan().getMode() == LogicalPlan.Mode.WRITE_ONLY) {
                Preconditions.checkArgument(
                        writeClient != null,
                        "write operations is unsupported in current execution engine");
                executor =
                        new GraphPlanExecutor() {
                            @Override
                            public void execute(
                                    GraphPlanner.Summary summary,
                                    IrMeta irMeta,
                                    ExecutionResponseListener listener) {
                                writeClient.submit(
                                        new ExecutionRequest(
                                                jobId,
                                                jobName,
                                                summary.getLogicalPlan(),
                                                summary.getPhysicalPlan()),
                                        listener,
                                        irMeta,
                                        timeoutConfig,
                                        statusCallback.getQueryLogger());
                            }
                        };
            } else {
                executor =
                        new GraphPlanExecutor() {
                            @Override
                            public void execute(
                                    GraphPlanner.Summary summary,
                                    IrMeta meta,
                                    ExecutionResponseListener listener)
                                    throws Exception {
                                ExecutionRequest request =
                                        new ExecutionRequest(
                                                jobId,
                                                jobName,
                                                summary.getLogicalPlan(),
                                                summary.getPhysicalPlan());
                                client.submit(
                                        request,
                                        listener,
                                        timeoutConfig,
                                        statusCallback.getQueryLogger());
                                statusCallback
                                        .getQueryLogger()
                                        .info("[query][submitted]: physical IR submitted");
                            }
                        };
            }
            return StatementResults.connectVia(
                    new CypherPlanExecution(
                            planSummary, timeoutConfig, statusCallback, irMeta, executor),
                    new QuerySubject.BasicQuerySubject());
        } catch (FrontendException e) {
            e.getDetails().put("QueryId", jobId);
            statusCallback.onErrorEnd(e.getMessage());
            throw e;
        } catch (Throwable t) {
            statusCallback.onErrorEnd(t.getMessage());
            throw new RuntimeException(t);
        } finally {
            if (irMeta != null) {
                metaQueryCallback.afterExec(irMeta);
            }
        }
    }

    private QueryTimeoutConfig getQueryTimeoutConfig() {
        return new QueryTimeoutConfig(fabricConfig.getTransactionTimeout().toMillis());
    }

    private void logCacheHit(QueryCache.Key key, QueryCache.Value value) {
        GraphPlanner.PlannerInstance cacheInstance =
                (GraphPlanner.PlannerInstance) value.debugInfo.get("instance");
        if (cacheInstance != null && cacheInstance != key.instance) {
            QueryLogger queryLogger = key.instance.getQueryLogger();
            if (queryLogger != null) {
                queryLogger.info(
                        "query hit the cache, cached query id [ {} ], cached query statement [ {}"
                                + " ]",
                        cacheInstance.getQueryLogger() == null
                                ? 0L
                                : cacheInstance.getQueryLogger().getQueryId(),
                        cacheInstance.getQuery());
            }
        }
    }
}
