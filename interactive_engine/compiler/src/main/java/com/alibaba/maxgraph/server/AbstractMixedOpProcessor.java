/**
 * This file is referred and derived from project apache/tinkerpop
 *
 * <p>https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/op/traversal/TraversalOpProcessor.java
 *
 * <p>which has the following license:
 *
 * <p>Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.server;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.exception.RetryGremlinException;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.ResponseHandlerContext;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public abstract class AbstractMixedOpProcessor extends StandardOpProcessor {
    private static final long CHANNEL_WRITABLE_TIMEOUT = 10000;
    protected static final Logger logger = LoggerFactory.getLogger(AbstractMixedOpProcessor.class);

    protected boolean vertexCacheFlag;
    protected boolean fetchPropFlag;
    protected int resultIterationBatchSize;
    protected boolean globalPullGraphFlag;

    protected AbstractMixedOpProcessor(InstanceConfig instanceConfig) {
        this.vertexCacheFlag = instanceConfig.gremlinVertexCacheEnable();
        this.fetchPropFlag = instanceConfig.timelyFetchPropFlag();
        this.resultIterationBatchSize = instanceConfig.getTimelyResultIterationBatchSize();
        this.globalPullGraphFlag = instanceConfig.getGlobalPullGraphFlag();
    }

    @Override
    protected void evalOpInternal(
            Context context,
            Supplier<GremlinExecutor> gremlinExecutorSupplier,
            AbstractEvalOpProcessor.BindingSupplier bindingsSupplier)
            throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        final Settings settings = context.getSettings();

        final Map<String, Object> args = msg.getArgs();
        final String script = (String) args.get(Tokens.ARGS_GREMLIN);

        final String language =
                args.containsKey(Tokens.ARGS_LANGUAGE)
                        ? (String) args.get(Tokens.ARGS_LANGUAGE)
                        : null;
        final Bindings bindings = new SimpleBindings();

        // sessionless requests are always transaction managed, but in-session requests are
        // configurable.
        final boolean managedTransactionsForRequest =
                manageTransactions
                        ? true
                        : (Boolean) args.getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // timeout override
        final long seto =
                args.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT)
                        ? Long.parseLong(args.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT).toString())
                        : settings.scriptEvaluationTimeout;

        logger.info("Receive query=>" + script);
        if (StringUtils.isEmpty(script) || StringUtils.equalsIgnoreCase(script, "''")) {
            logger.info("Finish empty query query=>" + script);
            writeResultList(context, Lists.newArrayList(), ResponseStatusCode.SUCCESS);
            return;
        }
        GremlinExecutor.LifeCycle timelyLifeCycle =
                createTimelyLifeCycle(
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
                        ctx);
        CompletableFuture<Object> evalFuture =
                gremlinExecutor.eval(script, language, bindings, timelyLifeCycle);
        evalFuture.handle(
                (v, t) -> {
                    timerContext.stop();
                    if (t != null) {
                        if (t instanceof RetryGremlinException) {
                            queryFromGremlin(
                                    timerContext,
                                    t,
                                    seto,
                                    managedTransactionsForRequest,
                                    msg,
                                    context,
                                    settings,
                                    bindingsSupplier,
                                    ctx,
                                    gremlinExecutor,
                                    script,
                                    language,
                                    bindings);
                        } else {
                            String errorMessage;
                            if (t instanceof TimedInterruptTimeoutException) {
                                errorMessage =
                                        String.format(
                                                "A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider",
                                                msg);
                                logger.warn(errorMessage);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                .statusMessage(
                                                        "Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                                                .create());
                            } else if (t instanceof TimeoutException) {
                                errorMessage =
                                        String.format(
                                                "Response evaluation exceeded the configured threshold for request [%s] - %s",
                                                msg, t.getMessage());
                                logger.warn(errorMessage, t);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                .statusMessage(t.getMessage())
                                                .create());
                            } else {
                                logger.warn(
                                        String.format(
                                                "Exception processing a script on request [%s].",
                                                msg),
                                        t);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(
                                                        ResponseStatusCode
                                                                .SERVER_ERROR_SCRIPT_EVALUATION)
                                                .statusMessage(t.getMessage())
                                                .create());
                            }
                        }
                    }
                    return null;
                });
    }

    private void queryFromGremlin(
            Timer.Context timerContext,
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
        logger.warn("Build query flow fail and try to query from tinkerpop", t);
        GremlinExecutor.LifeCycle gremlinLifeCycle =
                createGremlinLifeCycle(
                        seto,
                        managedTransactionsForRequest,
                        msg,
                        context,
                        settings,
                        bindingsSupplier,
                        ctx);
        CompletableFuture<Object> gremlinFuture =
                gremlinExecutor.eval(script, language, bindings, gremlinLifeCycle);
        gremlinFuture.handle(
                (vv, tt) -> {
                    timerContext.stop();
                    if (tt != null) {
                        if (tt instanceof OpProcessorException) {
                            ctx.writeAndFlush(((OpProcessorException) tt).getResponseMessage());
                        } else {
                            String errorMessage;
                            if (tt instanceof TimedInterruptTimeoutException) {
                                errorMessage =
                                        String.format(
                                                "A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider",
                                                msg);
                                logger.warn(errorMessage);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                .statusMessage(
                                                        "Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                                                .create());
                            } else if (tt instanceof TimeoutException) {
                                errorMessage =
                                        String.format(
                                                "Response evaluation exceeded the configured threshold for request [%s] - %s",
                                                msg, t.getMessage());
                                logger.warn(errorMessage, tt);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                .statusMessage(t.getMessage())
                                                .create());
                            } else {
                                logger.warn(
                                        String.format(
                                                "Exception processing a script on request [%s].",
                                                msg),
                                        tt);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                .statusMessage(t.getMessage())
                                                .create());
                            }
                        }
                    }

                    return null;
                });
    }

    private GremlinExecutor.LifeCycle createTimelyLifeCycle(
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
            ChannelHandlerContext ctx) {
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(0L)
                .afterFailure(
                        (b, t) -> {
                            if (managedTransactionsForRequest)
                                attemptRollback(
                                        msg,
                                        context.getGraphManager(),
                                        settings.strictTransactionManagement);
                        })
                .beforeEval(
                        (b) -> {
                            try {
                                b.putAll(bindingsSupplier.get());
                            } catch (OpProcessorException var3) {
                                throw new RuntimeException(var3);
                            }
                        })
                .withResult(
                        (o) -> {
                            try {
                                processGraphTraversal(script, context, o, seto);
                            } catch (Exception e) {
                                logger.warn("query " + script + " fail.", e);
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(
                                                        ResponseStatusCode
                                                                .SERVER_ERROR_SCRIPT_EVALUATION)
                                                .statusMessage(e.getMessage())
                                                .create());
                            }
                        })
                .create();
    }

    private GremlinExecutor.LifeCycle createGremlinLifeCycle(
            long seto,
            boolean managedTransactionsForRequest,
            RequestMessage msg,
            Context context,
            Settings settings,
            BindingSupplier bindingsSupplier,
            ChannelHandlerContext ctx) {
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(seto)
                .afterFailure(
                        (b, t) -> {
                            if (managedTransactionsForRequest)
                                attemptRollback(
                                        msg,
                                        context.getGraphManager(),
                                        settings.strictTransactionManagement);
                        })
                .beforeEval(
                        (b) -> {
                            try {
                                b.putAll(bindingsSupplier.get());
                            } catch (OpProcessorException var3) {
                                throw new RuntimeException(var3);
                            }
                        })
                .withResult(
                        (o) -> {
                            Iterator itty = IteratorUtils.asIterator(o);
                            logger.debug(
                                    "Preparing to iterate results from - {} - in thread [{}]",
                                    msg,
                                    Thread.currentThread().getName());

                            String err;
                            try {
                                this.handleIterator(context, itty);
                            } catch (InterruptedException var12) {
                                logger.warn(
                                        String.format(
                                                "Interruption during result iteration on request [%s].",
                                                msg),
                                        var12);
                                err = var12.getMessage();
                                String errx =
                                        "Interruption of result iteration"
                                                + (null != err && !err.isEmpty()
                                                        ? " - " + err
                                                        : "");
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR)
                                                .statusMessage(errx)
                                                .create());
                                if (managedTransactionsForRequest) {
                                    attemptRollback(
                                            msg,
                                            context.getGraphManager(),
                                            settings.strictTransactionManagement);
                                }
                            } catch (Exception var13) {
                                logger.warn(
                                        String.format(
                                                "Exception processing a script on request [%s].",
                                                msg),
                                        var13);
                                err = var13.getMessage();
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR)
                                                .statusMessage(
                                                        null != err && !err.isEmpty()
                                                                ? err
                                                                : var13.getClass().getSimpleName())
                                                .create());
                                if (managedTransactionsForRequest) {
                                    attemptRollback(
                                            msg,
                                            context.getGraphManager(),
                                            settings.strictTransactionManagement);
                                }
                            }
                        })
                .create();
    }

    @Override
    protected void handleIterator(final ResponseHandlerContext rhc, final Iterator itty)
            throws InterruptedException {
        Context context = rhc.getContext();
        ChannelHandlerContext ctx = context.getChannelHandlerContext();
        RequestMessage msg = context.getRequestMessage();
        Settings settings = context.getSettings();
        MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;
        boolean managedTransactionsForRequest =
                this.manageTransactions
                        ? true
                        : (Boolean) msg.getArgs().getOrDefault("manageTransaction", false);
        if (!itty.hasNext()) {
            if (managedTransactionsForRequest) {
                attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);
            }

            rhc.writeAndFlush(
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.NO_CONTENT)
                            .statusAttributes(
                                    this.generateStatusAttributes(
                                            ctx,
                                            msg,
                                            ResponseStatusCode.NO_CONTENT,
                                            itty,
                                            settings))
                            .create());
        } else {
            int resultIterationBatchSize =
                    (Integer)
                            msg.optionalArgs("batchSize").orElse(settings.resultIterationBatchSize);
            List<Object> aggregate = new ArrayList(resultIterationBatchSize);
            boolean hasMore = itty.hasNext();

            while (hasMore) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                boolean forceFlush = this.isForceFlushed(ctx, msg, itty);
                if (aggregate.size() < resultIterationBatchSize && itty.hasNext() && !forceFlush) {
                    Object object = itty.next();
                    if (object instanceof Path) {
                        object = DetachedFactory.detach((Path) object, true);
                    }
                    aggregate.add(object);
                }

                if (!ctx.channel().isWritable()) {
                    if (!warnOnce) {
                        logger.warn(
                                "Pausing response writing as writeBufferHighWaterMark exceeded on {} - writing will continue once client has caught up",
                                msg);
                        warnOnce = true;
                    }

                    TimeUnit.MILLISECONDS.sleep(10L);
                } else if (forceFlush
                        || aggregate.size() == resultIterationBatchSize
                        || !itty.hasNext()) {
                    ResponseStatusCode code =
                            itty.hasNext()
                                    ? ResponseStatusCode.PARTIAL_CONTENT
                                    : ResponseStatusCode.SUCCESS;
                    Frame frame = null;

                    try {
                        frame =
                                makeFrame(
                                        rhc,
                                        msg,
                                        serializer,
                                        useBinary,
                                        aggregate,
                                        code,
                                        this.generateResultMetaData(ctx, msg, code, itty, settings),
                                        this.generateStatusAttributes(
                                                ctx, msg, code, itty, settings));
                    } catch (Exception var20) {
                        if (frame != null) {
                            frame.tryRelease();
                        }

                        if (managedTransactionsForRequest) {
                            attemptRollback(
                                    msg,
                                    context.getGraphManager(),
                                    settings.strictTransactionManagement);
                        }

                        return;
                    }

                    boolean moreInIterator = itty.hasNext();

                    try {
                        if (moreInIterator) {
                            aggregate = new ArrayList(resultIterationBatchSize);
                        } else {
                            if (managedTransactionsForRequest) {
                                attemptCommit(
                                        msg,
                                        context.getGraphManager(),
                                        settings.strictTransactionManagement);
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

    public static void writeResultList(
            final Context context, List<Object> resultList, ResponseStatusCode statusCode) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        //        logger.info("write " + resultList.size() + " result to context " + context + "
        // status code=>" + statusCode);
        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (resultList.isEmpty()) {
            ctx.writeAndFlush(
                    ResponseMessage.build(msg).code(ResponseStatusCode.NO_CONTENT).create());
            return;
        }

        // send back a page of results if batch size is met or if it's the end of the results being
        // iterated.
        // also check writeability of the channel to prevent OOME for slow clients.
        boolean retryOnce = false;
        while (true) {
            if (ctx.channel().isWritable()) {
                Frame frame = null;
                try {
                    frame =
                            makeFrame(
                                    ctx,
                                    msg,
                                    serializer,
                                    useBinary,
                                    resultList,
                                    statusCode,
                                    Collections.emptyMap(),
                                    Collections.emptyMap());
                    ctx.writeAndFlush(frame).get();
                    break;
                } catch (Exception e) {
                    if (frame != null) {
                        frame.tryRelease();
                    }
                    logger.error(
                            "write "
                                    + resultList.size()
                                    + " result to context "
                                    + context
                                    + " status code=>"
                                    + statusCode
                                    + " fail",
                            e);
                    throw new RuntimeException(e);
                }

            } else {
                if (retryOnce) {
                    String message =
                            "write result to context fail for context " + msg + " is too busy";
                    logger.error(message);
                    throw new RuntimeException(message);
                } else {
                    logger.warn(
                            "Pausing response writing as writeBufferHighWaterMark exceeded on "
                                    + msg
                                    + " - writing will continue once client has caught up");
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

    protected abstract void processGraphTraversal(
            String script, Context context, Object traversal, long timeout)
            throws RetryGremlinException;

    public abstract List<Object> processHttpGraphTraversal(
            String script, Object traversal, long timeout, FullHttpRequest request)
            throws Exception;
}
