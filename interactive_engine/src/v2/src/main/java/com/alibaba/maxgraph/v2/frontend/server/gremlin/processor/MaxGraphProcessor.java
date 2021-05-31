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
package com.alibaba.maxgraph.v2.frontend.server.gremlin.processor;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.DropVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.EdgeRelationEntity;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.SchemaManager;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
import com.alibaba.maxgraph.v2.common.frontend.result.ResultConvertUtils;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.executor.BroadcastQueryExecutor;
import com.alibaba.maxgraph.v2.frontend.compiler.executor.QueryExecutor;
import com.alibaba.maxgraph.v2.frontend.compiler.executor.SingleServerQueryExecutor;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.LogicalPlanOptimizer;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.v2.frontend.compiler.query.MaxGraphQuery;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphGremlinResultProcessor;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.config.GraphStoreType;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import com.alibaba.maxgraph.v2.frontend.exception.ServerRetryGremlinException;
import com.alibaba.maxgraph.v2.frontend.graph.CancelDataflow;
import com.alibaba.maxgraph.v2.frontend.graph.ShowProcessListQuery;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.driver.ser.AbstractMaxGraphGryoMessageSerializerV3d0;
import com.alibaba.maxgraph.v2.frontend.server.session.ClientSessionManager;
import com.alibaba.maxgraph.v2.frontend.server.session.GraphSessionFactory;
import com.alibaba.maxgraph.v2.frontend.utils.ClientUtil;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.ResponseHandlerContext;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * MaxGraph processor in gremlin server for query/manager script
 */
public class MaxGraphProcessor extends StandardOpProcessor {
    private static final int SCHEMA_TIMEOUT_SEC = 30;
    protected static final Logger logger = LoggerFactory.getLogger(MaxGraphProcessor.class);

    /**
     * The frontend config
     */
    private Configs configs;
    /**
     * The writer/reader factory
     */
    private GraphSessionFactory factory;
    /**
     * The schema fetcher who will fetch the snapshot id and schema
     */
    private SchemaFetcher schemaFetcher;
    /**
     * Reader proxy, contains rpc and refresh connection for remote store
     */
    private RoleClients<QueryStoreRpcClient> queryStoreClients;
    /**
     * Graph partition manager
     */
    private GraphPartitionManager partitionManager;
    /**
     * Query executor will send query to runtime and execute the given query
     */
    private QueryExecutor queryExecutor;
    /**
     * Graph writer context
     */
    private GraphWriterContext graphWriterContext;
    /**
     * Write results to client batch size
     */
    private int resultIterationBatchSize;

    public MaxGraphProcessor(Configs configs,
                             SchemaFetcher schemaFetcher,
                             GraphPartitionManager partitionManager,
                             RoleClients<QueryStoreRpcClient> queryStoreClients,
                             RoleClients<QueryExecuteRpcClient> queryExecuteClients,
                             RoleClients<QueryManageRpcClient> queryManageClients,
                             int executorCount,
                             GraphWriterContext graphWriterContext) {
        this.configs = configs;
        this.factory = new GraphSessionFactory(GraphStoreType.valueOf(StringUtils.upperCase(FrontendConfig.GRAPH_STORE_TYPE.get(this.configs))));
        this.schemaFetcher = schemaFetcher;
        this.partitionManager = partitionManager;
        this.queryStoreClients = queryStoreClients;
        this.graphWriterContext = graphWriterContext;
        String engineType = StringUtils.upperCase(CommonConfig.ENGINE_TYPE.get(this.configs));
        if (StringUtils.equals(engineType, "TIMELY")) {
            this.queryExecutor = new SingleServerQueryExecutor(configs, queryExecuteClients, queryManageClients, executorCount);
        } else {
            this.queryExecutor = new BroadcastQueryExecutor(configs, queryExecuteClients, queryManageClients, executorCount);
        }
        this.resultIterationBatchSize = FrontendConfig.RESULT_ITERATION_BATCH_SIZE.get(this.configs);
    }

    @Override
    protected void evalOpInternal(Context context, Supplier<GremlinExecutor> gremlinExecutorSupplier, AbstractEvalOpProcessor.BindingSupplier bindingsSupplier) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        final Settings settings = context.getSettings();

        final Map<String, Object> args = msg.getArgs();
        final String script = (String) args.get(Tokens.ARGS_GREMLIN);
        final Map<String, Object> argsBindings = (Map<String, Object>) args.get(Tokens.ARGS_BINDINGS);

        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : null;
        final Bindings bindings = new SimpleBindings();
        SnapshotSchema snapshotSchema = this.schemaFetcher.fetchSchema();
        SnapshotMaxGraph snapshotMaxGraph = new SnapshotMaxGraph();
        MaxGraphCache cache = new MaxGraphCache();
        MaxGraphWriter writer = factory.buildGraphWriter(new ClientSessionManager(context), snapshotSchema, graphWriterContext, cache);
        MaxGraphReader reader = factory.buildGraphReader(snapshotMaxGraph, writer, this.partitionManager, this.queryStoreClients, this.schemaFetcher, this.configs, cache);
        snapshotMaxGraph.initialize(reader, writer, this.schemaFetcher);
        bindings.put("graph", snapshotMaxGraph);
        bindings.put("g", snapshotMaxGraph.traversal());

        // sessionless requests are always transaction managed, but in-session requests are configurable.
        final boolean managedTransactionsForRequest = manageTransactions ?
                true : (Boolean) args.getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // timeout override
        final long seto = args.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT) ?
                Long.parseLong(args.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT).toString()) : (null != argsBindings && argsBindings.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT) ?
                Long.parseLong(argsBindings.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT).toString()) : settings.scriptEvaluationTimeout);

        final String queryId = ClientUtil.getQueryId(args, argsBindings, script);
        final List<String> queueNameList = ClientUtil.getQueueNameList(args, argsBindings);
        String queueName = (null != queueNameList && !queueNameList.isEmpty()) ? queueNameList.get(RandomUtils.nextInt(0, queueNameList.size())) : null;
        logger.info("Receive query " + queryId + " =>" + script + " and pick queue name " + queueName);

        if (StringUtils.isEmpty(script) || StringUtils.equalsIgnoreCase(script, "''")) {
            writeResultList(context, Lists.newArrayList(), ResponseStatusCode.NO_CONTENT);
            return;
        }

        GremlinExecutor.LifeCycle maxGraphLifeCycle = createMaxGraphLifeCycle(
                queryId,
                queueName,
                timerContext,
                gremlinExecutor,
                language,
                bindings,
                script,
                seto,
                managedTransactionsForRequest,
                msg,
                context,
                settings,
                bindingsSupplier,
                ctx,
                snapshotSchema,
                snapshotMaxGraph);
        CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, maxGraphLifeCycle);
        evalFuture.handle((v, t) -> {
            timerContext.stop();
            if (t != null) {
                if (t instanceof ServerRetryGremlinException) {
                    queryFromGremlin(timerContext, t, seto, managedTransactionsForRequest, msg, context, settings, bindingsSupplier, ctx, gremlinExecutor, script, language, bindings);
                } else {
                    String errorMessage;
                    if (t instanceof TimedInterruptTimeoutException) {
                        errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider").create());
                    } else if (t instanceof TimeoutException) {
                        errorMessage = String.format("Response evaluation exceeded the configured threshold for request [%s] - %s", msg, t.getMessage());
                        logger.warn(errorMessage, t);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(t.getMessage()).create());
                    } else {
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), t);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION).statusMessage(t.getMessage()).create());
                    }
                }
            }
            return null;
        });
    }

    /**
     * Execute the query by gremlin engine
     *
     * @param timerContext                  The given timer context
     * @param t                             The given throable
     * @param seto                          THe given timeout(millisec)
     * @param managedTransactionsForRequest The given managedTransactionsForRequest
     * @param msg                           The given request message
     * @param context                       The given gremlin server context
     * @param settings                      The given settings
     * @param bindingsSupplier              jThe given bindings
     * @param ctx                           The given channel handler context
     * @param gremlinExecutor               The given executor
     * @param script                        The given script
     * @param language                      The given language(groovy by default)
     * @param bindings                      The given bindings
     */
    private void queryFromGremlin(Timer.Context timerContext,
                                  Throwable t,
                                  long seto,
                                  boolean managedTransactionsForRequest,
                                  RequestMessage msg,
                                  Context context,
                                  Settings settings,
                                  BindingSupplier bindingsSupplier,
                                  ChannelHandlerContext ctx,
                                  GremlinExecutor gremlinExecutor,
                                  String script,
                                  String language,
                                  Bindings bindings) {
        logger.warn("Execute query fail and try to query from tinkerpop", t);
        GremlinExecutor.LifeCycle gremlinLifeCycle = createGremlinLifeCycle(seto, managedTransactionsForRequest, msg, context, settings, bindingsSupplier, ctx);
        CompletableFuture<Object> gremlinFuture = gremlinExecutor.eval(script, language, bindings, gremlinLifeCycle);
        gremlinFuture.handle((vv, tt) -> {
            timerContext.stop();
            if (tt != null) {
                if (tt instanceof OpProcessorException) {
                    ctx.writeAndFlush(((OpProcessorException) tt).getResponseMessage());
                } else {
                    String errorMessage;
                    if (tt instanceof TimedInterruptTimeoutException) {
                        errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider").create());
                    } else if (tt instanceof TimeoutException) {
                        errorMessage = String.format("Response evaluation exceeded the configured threshold for request [%s] - %s", msg, t.getMessage());
                        logger.warn(errorMessage, tt);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(t.getMessage()).create());
                    } else {
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), tt);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(t.getMessage()).create());
                    }
                }
            }

            return null;
        });
    }

    /**
     * Create gremlin manager for max graph
     *
     * @param timerContext                  The given timer context
     * @param seto                          THe given timeout(millisec)
     * @param managedTransactionsForRequest The given managedTransactionsForRequest
     * @param msg                           The given request message
     * @param context                       The given gremlin server context
     * @param settings                      The given settings
     * @param bindingsSupplier              jThe given bindings
     * @param ctx                           The given channel handler context
     * @param gremlinExecutor               The given executor
     * @param script                        The given script
     * @param language                      The given language(groovy by default)
     * @param bindings                      The given bindings
     * @return The result lifecycle
     */
    private GremlinExecutor.LifeCycle createMaxGraphLifeCycle(
            String queryId,
            String queueName,
            Timer.Context timerContext,
            GremlinExecutor gremlinExecutor,
            String language,
            Bindings bindings,
            String script,
            long seto,
            boolean managedTransactionsForRequest,
            RequestMessage msg,
            Context context,
            Settings settings,
            BindingSupplier bindingsSupplier,
            ChannelHandlerContext ctx,
            SnapshotSchema snapshotSchema,
            SnapshotMaxGraph snapshotMaxGraph) {
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(0L)
                .afterFailure((b, t) -> {
                    if (managedTransactionsForRequest) {
                        attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                    }
                }).beforeEval((b) -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                    } catch (OpProcessorException var3) {
                        throw new RuntimeException(var3);
                    }
                }).withResult((o) -> {
                    try {
                        processGraphTraversal(script, context, o, seto, queryId, queueName, snapshotSchema, snapshotMaxGraph);
                    } catch (ServerRetryGremlinException e) {
                        queryFromGremlin(timerContext, e, seto, managedTransactionsForRequest, msg, context, settings, bindingsSupplier, ctx, gremlinExecutor, script, language, bindings);
                    } catch (Exception e) {
                        logger.warn("query " + script + " fail.", e);
                        String exceptionMessage = StringUtils.join(ExceptionUtils.getRootCauseStackTrace(e), "\n");
                        ctx.writeAndFlush(ResponseMessage.build(msg)
                                .code(ResponseStatusCode.SERVER_ERROR)
                                .statusMessage(exceptionMessage)
                                .result(exceptionMessage)
                                .create());
                    }
                }).create();
    }

    private GremlinExecutor.LifeCycle createGremlinLifeCycle(long seto,
                                                             boolean managedTransactionsForRequest,
                                                             RequestMessage msg,
                                                             Context context,
                                                             Settings settings,
                                                             BindingSupplier bindingsSupplier,
                                                             ChannelHandlerContext ctx) {
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(seto)
                .afterFailure((b, t) -> {
                    if (managedTransactionsForRequest) {
                        attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                    }
                }).beforeEval((b) -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                    } catch (OpProcessorException var3) {
                        throw new RuntimeException(var3);
                    }
                }).withResult((o) -> {
                    Iterator itty = IteratorUtils.asIterator(o);
                    logger.debug("Preparing to iterate results from - {} - in thread [{}]", msg, Thread.currentThread().getName());

                    String err;
                    try {
                        this.handleIterator(context, itty);
                    } catch (InterruptedException var12) {
                        logger.warn(String.format("Interruption during result iteration on request [%s].", msg), var12);
                        err = var12.getMessage();
                        String errx = "Interruption of result iteration" + (null != err && !err.isEmpty() ? " - " + err : "");
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(errx).create());
                        if (managedTransactionsForRequest) {
                            attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                        }
                    } catch (Exception var13) {
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), var13);
                        err = var13.getMessage();
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(null != err && !err.isEmpty() ? err : var13.getClass().getSimpleName()).create());
                        if (managedTransactionsForRequest) {
                            attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                        }
                    }

                }).create();
    }

    @Override
    protected void handleIterator(final ResponseHandlerContext rhc, final Iterator itty) throws InterruptedException {
        Context context = rhc.getContext();
        ChannelHandlerContext ctx = context.getChannelHandlerContext();
        RequestMessage msg = context.getRequestMessage();
        Settings settings = context.getSettings();
        MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;
        boolean managedTransactionsForRequest = this.manageTransactions ? true : (Boolean) msg.getArgs().getOrDefault("manageTransaction", false);
        if (!itty.hasNext()) {
            if (managedTransactionsForRequest) {
                attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);
            }

            rhc.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.NO_CONTENT).statusAttributes(this.generateStatusAttributes(ctx, msg, ResponseStatusCode.NO_CONTENT, itty, settings)).create());
        } else {
            int resultIterationBatchSize = (Integer) msg.optionalArgs("batchSize").orElse(settings.resultIterationBatchSize);
            List<Object> aggregate = new ArrayList(resultIterationBatchSize);
            boolean hasMore = itty.hasNext();

            while (hasMore) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                boolean forceFlush = this.isForceFlushed(ctx, msg, itty);
                if (aggregate.size() < resultIterationBatchSize && itty.hasNext() && !forceFlush) {
                    Object object = itty.next();
                    aggregate.add(ResultConvertUtils.convertGremlinResult(object));
                }

                if (!ctx.channel().isWritable()) {
                    if (!warnOnce) {
                        logger.warn("Pausing response writing as writeBufferHighWaterMark exceeded on {} - writing will continue once client has caught up", msg);
                        warnOnce = true;
                    }

                    TimeUnit.MILLISECONDS.sleep(10L);
                } else if (forceFlush || aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    ResponseStatusCode code = itty.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;
                    Frame frame = null;

                    try {
                        frame = makeFrame(rhc, msg, serializer, useBinary, aggregate, code, this.generateResultMetaData(ctx, msg, code, itty, settings), this.generateStatusAttributes(ctx, msg, code, itty, settings));
                    } catch (Exception var20) {
                        if (frame != null) {
                            frame.tryRelease();
                        }

                        if (managedTransactionsForRequest) {
                            attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                        }

                        return;
                    }

                    boolean moreInIterator = itty.hasNext();

                    try {
                        if (moreInIterator) {
                            aggregate = new ArrayList(resultIterationBatchSize);
                        } else {
                            if (managedTransactionsForRequest) {
                                attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);
                            }

                            hasMore = false;
                        }
                    } catch (Exception var19) {
                        if (frame != null) {
                            frame.tryRelease();
                        }

                        throw var19;
                    }

                    if (!moreInIterator) {
                        this.iterateComplete(ctx, msg, itty);
                    }

                    rhc.writeAndFlush(code, frame);
                }
            }

        }
    }

    /**
     * Write the results to the client with the given status code and gremlin server context
     *
     * @param context    The given gremlin server context for the current query
     * @param results    The results of the query
     * @param statusCode The status code for the current writing results
     */
    public static void writeResultList(Context context, List<Object> results, ResponseStatusCode statusCode) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final List<Object> resultList = serializer instanceof AbstractMaxGraphGryoMessageSerializerV3d0 ? results : results.stream().map(ResultConvertUtils::convertGremlinResult).collect(Collectors.toList());
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (resultList.isEmpty()) {
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.NO_CONTENT).create());
            return;
        }

        // send back a page of results if batch size is met or if it's the end of the results being iterated.
        // also check writeability of the channel to prevent OOME for slow clients.
        boolean retryOnce = false;
        while (true) {
            if (ctx.channel().isWritable()) {
                Frame frame = null;
                try {
                    frame = makeFrame(ctx, msg, serializer, useBinary, resultList, statusCode, Collections.emptyMap(), Collections.emptyMap());
                    ctx.writeAndFlush(frame);
                    break;
                } catch (Exception e) {
                    if (frame != null) {
                        frame.tryRelease();
                    }
                    logger.error("write " + resultList.size() + " result to context " + context + " status code=>" + statusCode + " fail", e);
                    throw new RuntimeException(e);
                }

            } else {
                if (retryOnce) {
                    String message = "Too many results are written for context " + msg + ", maybe you should add limit operator to get less results of the query";
                    logger.error(message);
                    throw new RuntimeException(message);
                } else {
                    logger.warn("Pausing response writing as writeBufferHighWaterMark exceeded on " + msg + " - writing will continue once client has caught up");
                    retryOnce = true;
                    try {
                        TimeUnit.MILLISECONDS.sleep(10L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * Process the query traversal
     *
     * @param script           The given query script
     * @param context          The given server context
     * @param traversal        The given query traversal
     * @param timeout          The given timeout
     * @param queryId          The given query id
     * @param queueName        The given queue name
     * @param snapshotSchema   The given snapshot schema
     * @param snapshotMaxGraph The given SnapshotMaxGraph
     */
    private void processGraphTraversal(String script,
                                       Context context,
                                       Object traversal,
                                       long timeout,
                                       String queryId,
                                       String queueName,
                                       SnapshotSchema snapshotSchema,
                                       SnapshotMaxGraph snapshotMaxGraph) throws Exception {
        if (traversal instanceof GraphTraversal) {
            final int resultIterationBatchSize = (Integer) context.getRequestMessage().optionalArgs(Tokens.ARGS_BATCH_SIZE)
                    .orElse(this.resultIterationBatchSize);
            MaxGraphQuery maxGraphQuery;
            try {
                MaxGraphResultProcessor resultProcessor = new MaxGraphGremlinResultProcessor(context, resultIterationBatchSize, snapshotMaxGraph);
                LogicalPlanOptimizer planOptimizer = new LogicalPlanOptimizer(snapshotSchema.getSchema(), snapshotSchema.getSnapshotId());
                QueryFlowManager queryFlowManager = planOptimizer.build((GraphTraversal) traversal);
                maxGraphQuery = new MaxGraphQuery(queryId, snapshotMaxGraph, snapshotSchema.getSchema(), queryFlowManager, resultProcessor);
            } catch (Exception e) {
                logger.error("Build query plan failed", e);
                throw new ServerRetryGremlinException(ExceptionUtils.getStackTrace(e));
            }
            this.queryExecutor.execute(maxGraphQuery, timeout);
        } else if (traversal instanceof ShowProcessListQuery) {
            final int resultIterationBatchSize = (Integer) context.getRequestMessage().optionalArgs(Tokens.ARGS_BATCH_SIZE)
                    .orElse(this.resultIterationBatchSize);
            MaxGraphResultProcessor resultProcessor = new MaxGraphGremlinResultProcessor(context, resultIterationBatchSize, snapshotMaxGraph);
            this.queryExecutor.showProcessList(resultProcessor);
        } else if (traversal instanceof CancelDataflow) {
            final int resultIterationBatchSize = (Integer) context.getRequestMessage().optionalArgs(Tokens.ARGS_BATCH_SIZE)
                    .orElse(this.resultIterationBatchSize);
            MaxGraphResultProcessor resultProcessor = new MaxGraphGremlinResultProcessor(context, resultIterationBatchSize, snapshotMaxGraph);
            CancelDataflow cancelDataflow = (CancelDataflow) traversal;
            this.queryExecutor.cancelDataflow(resultProcessor, cancelDataflow.getQueryId(), timeout);
        } else {
            if (traversal instanceof SchemaManager) {
                String resultMessage = processSchemaManager(traversal, snapshotSchema, snapshotMaxGraph.getGraphWriter());
                writeResultList(context,
                        Lists.newArrayList(resultMessage),
                        ResponseStatusCode.SUCCESS);
            } else if (traversal instanceof List) {
                writeResultList(context,
                        (List) ResultConvertUtils.convertGremlinResult(traversal),
                        ResponseStatusCode.SUCCESS);
            } else {
                writeResultList(context,
                        Lists.newArrayList(ResultConvertUtils.convertGremlinResult(traversal)),
                        ResponseStatusCode.SUCCESS);
            }
        }
    }

    private String processSchemaManager(Object schemaManager, SnapshotSchema snapshotSchema, MaxGraphWriter writer) {
        try {
            if (schemaManager instanceof CreateEdgeTypeManager) {
                CreateEdgeTypeManager createEdgeTypeManager = (CreateEdgeTypeManager) schemaManager;
                GraphSchema schema = snapshotSchema.getSchema();
                writer.createEdgeType(createEdgeTypeManager.getLabel(),
                        createEdgeTypeManager.getPropertyDefinitions(),
                        createEdgeTypeManager.getRelationList().stream()
                                .map(p -> new DefaultEdgeRelation((VertexType) schema.getSchemaElement(p.getSourceLabel()), (VertexType) schema.getSchemaElement(p.getTargetLabel())))
                                .collect(Collectors.toList()))
                        .get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                return "create edge type " + createEdgeTypeManager.getLabel() + " success";
            } else if (schemaManager instanceof CreateVertexTypeManager) {
                CreateVertexTypeManager createVertexTypeManager = (CreateVertexTypeManager) schemaManager;
                writer.createVertexType(createVertexTypeManager.getLabel(),
                        createVertexTypeManager.getPropertyDefinitions(),
                        createVertexTypeManager.getPrimaryKeyList()).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                return "create vertex type " + createVertexTypeManager.getLabel() + " success";
            } else if (schemaManager instanceof AlterEdgeTypeManager) {
                AlterEdgeTypeManager alterEdgeTypeManager = (AlterEdgeTypeManager) schemaManager;
                for (GraphProperty property : alterEdgeTypeManager.getPropertyDefinitions()) {
                    writer.addProperty(alterEdgeTypeManager.getLabel(), property).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                for (String propertyName : alterEdgeTypeManager.getDropPropertyNames()) {
                    writer.dropProperty(alterEdgeTypeManager.getLabel(), propertyName).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                for (EdgeRelationEntity relationPair : alterEdgeTypeManager.getAddRelationList()) {
                    writer.addEdgeRelation(alterEdgeTypeManager.getLabel(), relationPair.getSourceLabel(), relationPair.getTargetLabel()).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                for (EdgeRelationEntity relationPair : alterEdgeTypeManager.getDropRelationList()) {
                    writer.dropEdgeRelation(alterEdgeTypeManager.getLabel(), relationPair.getSourceLabel(), relationPair.getTargetLabel()).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                return "alter edge " + alterEdgeTypeManager.getLabel() + " success";
            } else if (schemaManager instanceof AlterVertexTypeManager) {
                AlterVertexTypeManager alterVertexTypeManager = (AlterVertexTypeManager) schemaManager;
                for (GraphProperty property : alterVertexTypeManager.getPropertyDefinitions()) {
                    writer.addProperty(alterVertexTypeManager.getLabel(), property).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                for (String propertyName : alterVertexTypeManager.getDropPropertyNames()) {
                    writer.dropProperty(alterVertexTypeManager.getLabel(), propertyName).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                return "alter vertex " + alterVertexTypeManager.getLabel() + " success";
            } else if (schemaManager instanceof DropVertexTypeManager) {
                DropVertexTypeManager dropVertexTypeManager = (DropVertexTypeManager) schemaManager;
                writer.dropVertexType(dropVertexTypeManager.getLabel()).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                return "drop vertex type " + dropVertexTypeManager.getLabel() + " success";
            } else if (schemaManager instanceof DropEdgeTypeManager) {
                DropEdgeTypeManager dropEdgeTypeManager = (DropEdgeTypeManager) schemaManager;
                writer.dropEdgeType(dropEdgeTypeManager.getLabel()).get(SCHEMA_TIMEOUT_SEC, TimeUnit.SECONDS);
                return "drop edge type " + dropEdgeTypeManager.getLabel() + " success";
            } else {
                throw new GraphCreateSchemaException("Unsupport schema manager operation " + schemaManager);
            }
        } catch (GraphCreateSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new GraphCreateSchemaException(ExceptionUtils.getStackTrace(e));
        }
    }
}
