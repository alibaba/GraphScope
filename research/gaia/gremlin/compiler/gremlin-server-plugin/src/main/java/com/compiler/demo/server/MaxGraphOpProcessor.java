/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/op/AbstractEvalOpProcessor.java
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.compiler.demo.server;

import com.alibaba.pegasus.builder.JobBuilder;
import com.codahale.metrics.Timer;
import com.compiler.demo.server.broadcast.BroadcastProcessor;
import com.compiler.demo.server.broadcast.RpcBroadcastProcessor;
import com.compiler.demo.server.plan.PlanUtils;
import com.compiler.demo.server.plan.strategy.GraphTraversalStrategies;
import com.compiler.demo.server.plan.translator.TraversalTranslator;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class MaxGraphOpProcessor extends StandardOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphOpProcessor.class);
    private BroadcastProcessor broadcastProcessor;

    public MaxGraphOpProcessor() {
        this.broadcastProcessor = new RpcBroadcastProcessor("conf/engine.hosts");
    }

    @Override
    protected void evalOpInternal(Context ctx, Supplier<GremlinExecutor> gremlinExecutorSupplier,
                                  AbstractEvalOpProcessor.BindingSupplier bindingsSupplier) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final RequestMessage msg = ctx.getRequestMessage();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        final Settings settings = ctx.getSettings();

        final Map<String, Object> args = msg.getArgs();

        final String script = (String) args.get(Tokens.ARGS_GREMLIN);
        logger.info("script is {}", script);
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : null;
        final Bindings bindings = new SimpleBindings();

        final long seto = args.containsKey(Tokens.ARGS_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_EVAL_TIMEOUT)).longValue() : settings.getEvaluationTimeout();

        final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(seto)
                .beforeEval(b -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                        b.put("g", ctx.getGraphManager().getGraph("graph").traversal());
                    } catch (OpProcessorException ope) {
                        throw new RuntimeException(ope);
                    }
                })
                .transformResult(o -> {
                    applyStrategy((Traversal) o);
                    return o;
                })
                .withResult(o -> {
                    JobBuilder jobReqBuilder = new TraversalTranslator((Traversal.Admin) o).translate();
                    PlanUtils.print(jobReqBuilder);
                    broadcastProcessor.broadcast(jobReqBuilder.build(), ctx);
                }).create();

        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, lifeCycle);

        evalFuture.handle((v, t) -> {
            timerContext.stop();

            if (t != null) {
                if (t instanceof OpProcessorException) {
                    ctx.writeAndFlush(((OpProcessorException) t).getResponseMessage());
                } else if (t instanceof TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                            .statusAttributeException(t).create());
                } else if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Script evaluation exceeded the configured threshold for request [%s]", msg);
                    logger.warn(errorMessage, t);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage(t.getMessage())
                            .statusAttributeException(t).create());
                } else {
                    if (t instanceof MultipleCompilationErrorsException && t.getMessage().contains("Method too large") &&
                            ((MultipleCompilationErrorsException) t).getErrorCollector().getErrorCount() == 1) {
                        final String errorMessage = String.format("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM, please split it into multiple smaller statements");
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(errorMessage)
                                .statusAttributeException(t).create());
                    } else {
                        final String errorMessage = (t.getMessage() == null) ? t.toString() : t.getMessage();
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), t);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(errorMessage)
                                .statusAttributeException(t).create());
                    }
                }
            }
            return null;
        });
    }

    public static void writeResultList(final Context context, List<Object> resultList, final ResponseStatusCode statusCode) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        if (statusCode == ResponseStatusCode.SERVER_ERROR) {
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).create());
        }
        if (resultList.isEmpty()) {
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.NO_CONTENT).create());
            return;
        }

        boolean retryOnce = false;
        while (true) {
            if (ctx.channel().isWritable()) {
                Frame frame = null;
                try {
                    frame = makeFrame(ctx, msg, serializer, useBinary, resultList, statusCode, Collections.emptyMap(), Collections.emptyMap());
                    ctx.writeAndFlush(frame).get();
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
                    String message = "write result to context fail for context " + msg + " is too busy";
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

    public static void applyStrategy(Traversal traversal) {
        traversal.asAdmin().setStrategies(GraphTraversalStrategies.instance());
        traversal.asAdmin().applyStrategies();
    }

    @Override
    public String getName() {
        return "maxgraph";
    }
}
