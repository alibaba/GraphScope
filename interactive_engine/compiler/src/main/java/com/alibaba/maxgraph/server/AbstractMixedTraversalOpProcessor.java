/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/op/traversal/TraversalOpProcessor.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.server;

import static com.codahale.metrics.MetricRegistry.name;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.step.MaxGraphIoStep;
import com.alibaba.maxgraph.sdkcommon.graph.DfsRequest;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.netty.channel.ChannelHandlerContext;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.script.SimpleBindings;

public abstract class AbstractMixedTraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMixedOpProcessor.class);
    private static final ObjectMapper mapper =
            GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper();
    private static final String OP_PROCESSOR_NAME = "traversal";
    private static final Timer traversalOpTimer =
            MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traversal"));

    private static final Settings.ProcessorSettings DEFAULT_SETTINGS =
            new Settings.ProcessorSettings();

    /**
     * Configuration setting for how long a cached side-effect will be available before it is evicted from the cache.
     */
    private static final String CONFIG_CACHE_EXPIRATION_TIME = "cacheExpirationTime";

    /**
     * Default timeout for a cached side-effect is ten minutes.
     */
    private static final long DEFAULT_CACHE_EXPIRATION_TIME = 600000;

    /**
     * Configuration setting for the maximum number of entries the cache will have.
     */
    private static final String CONFIG_CACHE_MAX_SIZE = "cacheMaxSize";

    /**
     * Default size of the max size of the cache.
     */
    private static final long DEFAULT_CACHE_MAX_SIZE = 1000;

    static {
        DEFAULT_SETTINGS.className = TraversalOpProcessor.class.getCanonicalName();
        DEFAULT_SETTINGS.config =
                new HashMap<String, Object>() {
                    {
                        put(CONFIG_CACHE_EXPIRATION_TIME, DEFAULT_CACHE_EXPIRATION_TIME);
                        put(CONFIG_CACHE_MAX_SIZE, DEFAULT_CACHE_MAX_SIZE);
                    }
                };
    }

    private static com.github.benmanes.caffeine.cache.Cache<UUID, TraversalSideEffects> cache =
            null;

    protected boolean vertexCacheFlag;
    protected int resultIterationBatchSize;

    public AbstractMixedTraversalOpProcessor(InstanceConfig instanceConfig) {
        super(false);
        this.vertexCacheFlag = instanceConfig.gremlinVertexCacheEnable();
        this.resultIterationBatchSize = instanceConfig.getTimelyResultIterationBatchSize();
    }

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public void close() throws Exception {
        // do nothing = no resources to release
    }

    @Override
    public void init(final Settings settings) {
        final Settings.ProcessorSettings processorSettings =
                settings.processors.stream()
                        .filter(
                                p ->
                                        p.className.equals(
                                                TraversalOpProcessor.class.getCanonicalName()))
                        .findAny()
                        .orElse(DEFAULT_SETTINGS);
        final long maxSize =
                Long.parseLong(processorSettings.config.get(CONFIG_CACHE_MAX_SIZE).toString());
        final long expirationTime =
                Long.parseLong(
                        processorSettings.config.get(CONFIG_CACHE_EXPIRATION_TIME).toString());

        cache =
                Caffeine.newBuilder()
                        .expireAfterWrite(expirationTime, TimeUnit.MILLISECONDS)
                        .maximumSize(maxSize)
                        .build();

        logger.info(
                "Initialized cache for {} with size {} and expiration time of {} ms",
                TraversalOpProcessor.class.getSimpleName(),
                maxSize,
                expirationTime);
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                validateTraversalSourceAlias(ctx, message, validateTraversalRequest(message));
                op = this::iterateBytecodeTraversal;
                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid =
                        String.format(
                                "Message could not be parsed.  Check the format of the request."
                                        + " [%s]",
                                message);
                throw new OpProcessorException(
                        msgInvalid,
                        ResponseMessage.build(message)
                                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                                .statusMessage(msgInvalid)
                                .create());
            default:
                final String msgDefault =
                        String.format(
                                "Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(
                        msgDefault,
                        ResponseMessage.build(message)
                                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                                .statusMessage(msgDefault)
                                .create());
        }

        return op;
    }

    private static void validateTraversalSourceAlias(
            final Context ctx, final RequestMessage message, final Map<String, String> aliases)
            throws OpProcessorException {
        final String traversalSourceBindingForAlias = aliases.values().iterator().next();
        if (!ctx.getGraphManager()
                .getTraversalSourceNames()
                .contains(traversalSourceBindingForAlias)) {
            final String msg =
                    String.format(
                            "The traversal source [%s] for alias [%s] is not configured on the"
                                    + " server.",
                            traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(
                    msg,
                    ResponseMessage.build(message)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                            .statusMessage(msg)
                            .create());
        }
    }

    private static Map<String, String> validateTraversalRequest(final RequestMessage message)
            throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg =
                    String.format(
                            "A message with [%s] op code requires a [%s] argument.",
                            Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(
                    msg,
                    ResponseMessage.build(message)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                            .statusMessage(msg)
                            .create());
        }

        final Optional<Map<String, String>> aliases = validatedAliases(message);

        return aliases.get();
    }

    private static Optional<Map<String, String>> validatedAliases(final RequestMessage message)
            throws OpProcessorException {
        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg =
                    String.format(
                            "A message with [%s] op code requires a [%s] argument.",
                            Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(
                    msg,
                    ResponseMessage.build(message)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                            .statusMessage(msg)
                            .create());
        }

        if (aliases.get().size() != 1
                || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
            final String msg =
                    String.format(
                            "A message with [%s] op code requires the [%s] argument to be a Map"
                                    + " containing one alias assignment named '%s'.",
                            Tokens.OPS_BYTECODE,
                            Tokens.ARGS_ALIASES,
                            Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(
                    msg,
                    ResponseMessage.build(message)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                            .statusMessage(msg)
                            .create());
        }

        return aliases;
    }

    private void iterateBytecodeTraversal(final Context context)
            throws OpProcessorException, Exception {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug(
                "Traversal request {} for in thread {}",
                msg.getRequestId(),
                Thread.currentThread().getName());

        // right now the TraversalOpProcessor can take a direct GraphSON representation of Bytecode
        // or directly take
        // deserialized Bytecode object.
        final Object bytecodeObj = msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final long timeout =
                msg.getArgs().containsKey(Tokens.ARGS_EVAL_TIMEOUT)
                        ? Long.parseLong(msg.getArgs().get(Tokens.ARGS_EVAL_TIMEOUT).toString())
                        : context.getSettings().evaluationTimeout;
        final Bytecode bytecode =
                bytecodeObj instanceof DfsRequest
                        ? Bytecode.class.cast(((DfsRequest) bytecodeObj).getBytecode())
                        : bytecodeObj instanceof Bytecode
                                ? (Bytecode) bytecodeObj
                                : mapper.readValue(bytecodeObj.toString(), Bytecode.class);

        // earlier validation in selection of this op method should free us to cast this without
        // worry
        final Map<String, String> aliases =
                (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSource(traversalSourceName);

        final Traversal.Admin<?, ?> traversal;
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent()) traversal = JavaTranslator.of(g).translate(bytecode);
            else {
                final GremlinExecutor engines = context.getGremlinExecutor();
                final SimpleBindings b = new SimpleBindings();
                b.put(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS, g);
                traversal = engines.eval(bytecode, b, lambdaLanguage.get(), traversalSourceName);
            }
        } catch (Exception ex) {
            logger.error("Could not deserialize the Traversal instance", context);
            throw new OpProcessorException(
                    "Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage())
                            .create());
        }

        for (final IoStep originalIoStep :
                TraversalHelper.getStepsOfClass(IoStep.class, traversal)) {
            MaxGraphIoStep maxGraphIoStep =
                    new MaxGraphIoStep<>(originalIoStep.getTraversal(), originalIoStep.getFile());
            maxGraphIoStep.setMode(originalIoStep.getMode());
            TraversalHelper.replaceStep(originalIoStep, maxGraphIoStep, traversal);
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();

            context.getGremlinExecutor()
                    .getExecutorService()
                    .submit(
                            () -> {
                                try {
                                    beforeProcessing(graph, context);

                                    try {
                                        if (bytecodeObj instanceof DfsRequest) {
                                            processTraversal(
                                                    context,
                                                    createDfsTraversal(
                                                            GraphTraversal.Admin.class.cast(
                                                                    traversal),
                                                            DfsRequest.class.cast(bytecodeObj)),
                                                    graph,
                                                    timeout);
                                        } else {
                                            processTraversal(context, traversal, graph, timeout);
                                        }
                                    } catch (TimeoutException ex) {
                                        final String errorMessage =
                                                String.format(
                                                        "Response iteration exceeded the configured"
                                                            + " threshold for request [%s] - %s",
                                                        msg.getRequestId(), ex.getMessage());
                                        logger.warn(errorMessage);
                                        ctx.writeAndFlush(
                                                ResponseMessage.build(msg)
                                                        .code(
                                                                ResponseStatusCode
                                                                        .SERVER_ERROR_TIMEOUT)
                                                        .statusMessage(errorMessage)
                                                        .create());
                                        onError(graph, context);
                                        return;
                                    } catch (Exception ex) {
                                        logger.warn(
                                                String.format(
                                                        "Exception processing a Traversal on"
                                                                + " iteration for request [%s].",
                                                        msg.getRequestId()),
                                                ex);
                                        ctx.writeAndFlush(
                                                ResponseMessage.build(msg)
                                                        .code(ResponseStatusCode.SERVER_ERROR)
                                                        .statusMessage(ex.getMessage())
                                                        .create());
                                        onError(graph, context);
                                        return;
                                    }
                                } catch (Exception ex) {
                                    logger.warn(
                                            String.format(
                                                    "Exception processing a Traversal on request"
                                                            + " [%s].",
                                                    msg.getRequestId()),
                                            ex);
                                    ctx.writeAndFlush(
                                            ResponseMessage.build(msg)
                                                    .code(ResponseStatusCode.SERVER_ERROR)
                                                    .statusMessage(ex.getMessage())
                                                    .create());
                                    onError(graph, context);
                                } finally {
                                    timerContext.stop();
                                }
                            });

        } catch (Exception ex) {
            timerContext.stop();
            throw new OpProcessorException(
                    "Could not iterate the Traversal instance",
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(ex.getMessage())
                            .create());
        }
    }

    protected abstract Object createDfsTraversal(
            GraphTraversal.Admin<?, ?> traversal, DfsRequest dfsRequest);

    protected abstract void processTraversal(
            Context context, Object traversal, Graph graph, long timeout)
            throws TimeoutException, InterruptedException;

    @Override
    protected void iterateComplete(
            final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        if (itty instanceof TraverserIterator) {
            final Traversal.Admin traversal = ((TraverserIterator) itty).getTraversal();
            if (!traversal.getSideEffects().isEmpty()) {
                cache.put(msg.getRequestId(), traversal.getSideEffects());
            }
        }
    }

    private void beforeProcessing(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen())
            graph.tx().rollback();
    }

    private void onError(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen())
            graph.tx().rollback();
    }

    private void onTraversalSuccess(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen())
            graph.tx().commit();
    }

    private void onSideEffectSuccess(final Graph graph, final Context ctx) {
        // there was no "writing" here, just side-effect retrieval, so if a transaction was opened
        // then
        // just close with rollback
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen())
            graph.tx().rollback();
    }
}
