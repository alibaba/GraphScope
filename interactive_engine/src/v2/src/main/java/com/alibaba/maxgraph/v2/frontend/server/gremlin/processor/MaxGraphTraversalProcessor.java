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
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
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
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphTraverserResultProcessor;
import com.alibaba.maxgraph.v2.frontend.compiler.step.MaxGraphIoStep;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.config.GraphStoreType;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.io.MaxGraphGryoReader;
import com.alibaba.maxgraph.v2.frontend.server.session.ClientSessionManager;
import com.alibaba.maxgraph.v2.frontend.server.session.GraphSessionFactory;
import com.alibaba.maxgraph.v2.frontend.utils.ClientUtil;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.SideEffectIterator;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class MaxGraphTraversalProcessor extends TraversalOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphTraversalProcessor.class);
    private static final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper();

    private static final Bindings EMPTY_BINDINGS = new SimpleBindings();

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

    public MaxGraphTraversalProcessor(Configs configs,
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
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                op = this::iterateBytecodeTraversal;
                break;
            case Tokens.OPS_GATHER:
                final Optional<String> sideEffectForGather = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForGather.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                final Optional<String> sideEffectKey = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT_KEY);
                if (!sideEffectKey.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT_KEY);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                validateTraversalSourceAlias(ctx, message, validatedAliases(message).get());

                op = this::gatherSideEffect;

                break;
            case Tokens.OPS_KEYS:
                final Optional<String> sideEffectForKeys = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForKeys.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = context -> {
                    final RequestMessage msg = context.getRequestMessage();
                    final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                    final TraversalSideEffects sideEffects = cache.getIfPresent(sideEffect.get());

                    if (null == sideEffects)
                        logger.warn("Request for side-effect keys on {} returned no side-effects in the cache", sideEffect.get());

                    handleIterator(context, null == sideEffects ? Collections.emptyIterator() : sideEffects.keys().iterator());
                };

                break;
            case Tokens.OPS_CLOSE:
                final Optional<String> sideEffectForClose = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForClose.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_CLOSE, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = context -> {
                    final RequestMessage msg = context.getRequestMessage();
                    logger.debug("Close request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

                    final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                    cache.invalidate(sideEffect.get());

                    final String successMessage = String.format("Successfully cleared side effect cache for [%s].", Tokens.ARGS_SIDE_EFFECT);
                    ctx.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(message).code(ResponseStatusCode.NO_CONTENT).statusMessage(successMessage).create());
                };

                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgInvalid).create());
            default:
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgDefault).create());
        }

        return op;
    }

    private void iterateBytecodeTraversal(final Context context) throws Exception {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // right now the TraversalOpProcessor can take a direct GraphSON representation of Bytecode or directly take
        // deserialized Bytecode object.
        final Object bytecodeObj = msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Bytecode bytecode = bytecodeObj instanceof Bytecode ? (Bytecode) bytecodeObj :
                mapper.readValue(bytecodeObj.toString(), Bytecode.class);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        // timeout override
        final long seto = msg.getArgs().containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT)
                // could be sent as an integer or long
                ? ((Number) msg.getArgs().get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT)).longValue()
                : context.getSettings().scriptEvaluationTimeout;

        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final SnapshotSchema snapshotSchema = this.schemaFetcher.fetchSchema();
        SnapshotMaxGraph snapshotMaxGraph = new SnapshotMaxGraph();
        MaxGraphCache cache = new MaxGraphCache();
        MaxGraphWriter writer = factory.buildGraphWriter(new ClientSessionManager(context), snapshotSchema, graphWriterContext, cache);
        MaxGraphReader reader = factory.buildGraphReader(snapshotMaxGraph, writer, this.partitionManager, this.queryStoreClients, this.schemaFetcher, this.configs, cache);
        snapshotMaxGraph.initialize(reader, writer, this.schemaFetcher);
        final TraversalSource g = snapshotMaxGraph.traversal();

        final Traversal.Admin<?, ?> traversal;
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                traversal = JavaTranslator.of(g).translate(bytecode);
            else
                traversal = context.getGremlinExecutor().eval(bytecode, EMPTY_BINDINGS, lambdaLanguage.get(), traversalSourceName);
        } catch (Exception ex) {
            logger.error("Could not deserialize the Traversal instance", ex);
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        }
        for (final IoStep originalIoStep : TraversalHelper.getStepsOfClass(IoStep.class, traversal)) {
            MaxGraphIoStep maxGraphIoStep = new MaxGraphIoStep<>(originalIoStep.getTraversal(), originalIoStep.getFile());
            maxGraphIoStep.setMode(originalIoStep.getMode());
            TraversalHelper.replaceStep(originalIoStep, maxGraphIoStep, traversal);
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();

            try {
                beforeProcessing(graph, context);

                try {
                    final int resultIterationBatchSize = (Integer) context.getRequestMessage().optionalArgs(Tokens.ARGS_BATCH_SIZE)
                            .orElse(this.resultIterationBatchSize);
                    final Map<String, Object> args = msg.getArgs();
                    final Map<String, Object> argsBindings = (Map<String, Object>) args.get(Tokens.ARGS_BINDINGS);

                    MaxGraphQuery maxGraphQuery;
                    final String queryId = ClientUtil.getQueryId(args, argsBindings, traversal.toString());

                    MaxGraphResultProcessor resultProcessor = new MaxGraphTraverserResultProcessor(context, resultIterationBatchSize, snapshotMaxGraph);
                    try {
                        LogicalPlanOptimizer planOptimizer = new LogicalPlanOptimizer(snapshotSchema.getSchema(), snapshotSchema.getSnapshotId());
                        QueryFlowManager queryFlowManager = planOptimizer.build((GraphTraversal) traversal);
                        maxGraphQuery = new MaxGraphQuery(queryId,
                                snapshotMaxGraph,
                                snapshotSchema.getSchema(),
                                queryFlowManager,
                                resultProcessor);
                    } catch (Exception e) {
                        List<Object> resultList = Lists.newArrayListWithCapacity(resultIterationBatchSize);
                        DetachedTraverserIterator traverserIterator = new DetachedTraverserIterator(traversal);
                        while (traverserIterator.hasNext()) {
                            resultList.add(traverserIterator.next());
                            if (resultList.size() >= resultIterationBatchSize) {
                                resultProcessor.process(resultList);
                                resultList.clear();
                            }
                        }
                        resultProcessor.process(resultList);
                        resultProcessor.finish();
                        return null;
                    }
                    this.queryExecutor.execute(maxGraphQuery, seto);
                } catch (Exception ex) {
                    Throwable t = ex;
                    if (ex instanceof UndeclaredThrowableException)
                        t = t.getCause();

                    if (t instanceof InterruptedException || t instanceof TraversalInterruptedException) {
                        final String errorMessage = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to scriptEvaluationTimeout", msg);
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                .statusMessage(errorMessage)
                                .statusAttributeException(ex).create());
                        onError(graph, context);
                    } else {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                .statusMessage(ex.getMessage())
                                .statusAttributeException(ex).create());
                        onError(graph, context);
                    }
                }
            } catch (Exception ex) {
                logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                        .statusMessage(ex.getMessage())
                        .statusAttributeException(ex).create());
                onError(graph, context);
            } finally {
                timerContext.stop();
            }

            return null;
        });

        final Future<?> executionFuture = context.getGremlinExecutor().getExecutorService().submit(evalFuture);
        if (seto > 0) {
            // Schedule a timeout in the thread pool for future execution
            context.getScheduledExecutorService().schedule(() -> executionFuture.cancel(true), seto, TimeUnit.MILLISECONDS);
        }
    }

    private static void validateTraversalSourceAlias(final Context ctx, final RequestMessage message, final Map<String, String> aliases) throws OpProcessorException {
        final String traversalSourceBindingForAlias = aliases.values().iterator().next();
        if (!ctx.getGraphManager().getTraversalSourceNames().contains(traversalSourceBindingForAlias)) {
            final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }
    }

    private static Map<String, String> validateTraversalRequest(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return validatedAliases(message).get();
    }

    private static Optional<Map<String, String>> validatedAliases(final RequestMessage message) throws OpProcessorException {
        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1 || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
            final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment named '%s'.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return aliases;
    }

    private void gatherSideEffect(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Side-effect request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // earlier validation in selection of this op method should free us to cast this without worry
        final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
        final Optional<String> sideEffectKey = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT_KEY);
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSource(traversalSourceName);

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    beforeProcessing(graph, context);

                    try {
                        final TraversalSideEffects sideEffects = cache.getIfPresent(sideEffect.get());

                        if (null == sideEffects) {
                            final String errorMessage = String.format("Could not find side-effects for %s.", sideEffect.get());
                            logger.warn(errorMessage);
                            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(errorMessage).create());
                            onError(graph, context);
                            return;
                        }

                        if (!sideEffects.exists(sideEffectKey.get())) {
                            final String errorMessage = String.format("Could not find side-effect key for %s in %s.", sideEffectKey.get(), sideEffect.get());
                            logger.warn(errorMessage);
                            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(errorMessage).create());
                            onError(graph, context);
                            return;
                        }

                        handleIterator(context, new SideEffectIterator(sideEffects.get(sideEffectKey.get()), sideEffectKey.get()));
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a side-effect on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                .statusMessage(ex.getMessage())
                                .statusAttributeException(ex).create());
                        onError(graph, context);
                        return;
                    }

                    onSideEffectSuccess(graph, context);
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a side-effect on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
                    onError(graph, context);
                } finally {
                    timerContext.stop();
                }
            });

        } catch (Exception ex) {
            timerContext.stop();
            throw new OpProcessorException("Could not iterate the side-effect instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        }
    }
}

class DetachedTraverserIterator implements Iterator<Object> {

    private final Traversal.Admin traversal;
    private final HaltedTraverserStrategy haltedTraverserStrategy;
    private final TraverserSet bulker = new TraverserSet();
    private final int barrierSize;

    public DetachedTraverserIterator(final Traversal.Admin traversal) {
        this.traversal = traversal;
        this.barrierSize = traversal.getTraverserRequirements().contains(TraverserRequirement.ONE_BULK) ? 1 : 1000;
        this.haltedTraverserStrategy = HaltedTraverserStrategy.detached();
    }

    public Traversal.Admin getTraversal() {
        return this.traversal;
    }

    @Override
    public boolean hasNext() {
        if (this.bulker.isEmpty())
            this.fillBulker();
        return !this.bulker.isEmpty();
    }

    @Override
    public Object next() {
        if (this.bulker.isEmpty())
            this.fillBulker();
        final Traverser.Admin t = this.haltedTraverserStrategy.halt(this.bulker.remove());
        return new DefaultRemoteTraverser<>(t.get(), t.bulk());
    }

    private final void fillBulker() {
        while (this.traversal.hasNext() && this.bulker.size() < this.barrierSize) {
            this.bulker.add(this.traversal.nextTraverser());
        }
    }
}
