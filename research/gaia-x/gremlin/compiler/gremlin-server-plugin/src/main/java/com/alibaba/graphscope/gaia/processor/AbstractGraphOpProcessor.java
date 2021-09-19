/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/op/AbstractEvalOpProcessor.java
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
package com.alibaba.graphscope.gaia.processor;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.idmaker.IncrementalQueryIdMaker;
import com.alibaba.graphscope.gaia.plan.strategy.GraphTraversalStrategies;
import com.alibaba.graphscope.gaia.plan.strategy.OrderGuaranteeStrategy;
import com.alibaba.graphscope.gaia.plan.strategy.PropertyShuffleStrategy;
import com.alibaba.graphscope.gaia.plan.strategy.global.PathHistoryStrategy;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.SchemaNotFoundException;
import com.codahale.metrics.Timer;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.PreCachePropertyStrategy;
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
import org.apache.tinkerpop.gremlin.server.ResponseHandlerContext;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
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

public abstract class AbstractGraphOpProcessor extends StandardOpProcessor {
    protected IdMaker queryIdMaker;
    private static final Logger logger = LoggerFactory.getLogger(GaiaGraphOpProcessor.class);
    protected GaiaConfig config;
    protected GraphStoreService graphStore;

    public AbstractGraphOpProcessor(GaiaConfig config, GraphStoreService graphStore) {
        this.queryIdMaker = new IncrementalQueryIdMaker();
        this.config = config;
        this.graphStore = graphStore;
    }

    protected abstract GremlinExecutor.LifeCycle createLifeCycle(Context ctx, Supplier<GremlinExecutor> gremlinExecutorSupplier, BindingSupplier bindingsSupplier);

    @Override
    protected void evalOpInternal(Context ctx, Supplier<GremlinExecutor> gremlinExecutorSupplier, BindingSupplier bindingsSupplier) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final RequestMessage msg = ctx.getRequestMessage();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();

        final Map<String, Object> args = msg.getArgs();

        final String script = (String) args.get(Tokens.ARGS_GREMLIN);
        logger.info("script is {}", script);
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : null;
        final Bindings bindings = new SimpleBindings();

        final GremlinExecutor.LifeCycle lifeCycle = createLifeCycle(ctx, gremlinExecutorSupplier, bindingsSupplier);
        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, lifeCycle);

        evalFuture.handle((v, t) -> {
            long elapsed = timerContext.stop();
            logger.info("query {} total execution time is {} ms", script, elapsed / 1000000.0f);
            ResponseHandlerContext rhc = new ResponseHandlerContext(ctx);
            if (t != null) {
                if (t instanceof OpProcessorException) {
                    rhc.writeAndFlush(((OpProcessorException) t).getResponseMessage());
                } else if (t instanceof TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    rhc.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                            .statusAttributeException(t).create());
                } else if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Script evaluation exceeded the configured threshold for request [%s]", msg);
                    logger.warn(errorMessage, t);
                    rhc.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage(t.getMessage())
                            .statusAttributeException(t).create());
                } else if (t instanceof SchemaNotFoundException) {
                    writeResultList(ctx, Collections.EMPTY_LIST, ResponseStatusCode.SUCCESS);
                } else {
                    if (t instanceof MultipleCompilationErrorsException && t.getMessage().contains("Method too large") &&
                            ((MultipleCompilationErrorsException) t).getErrorCollector().getErrorCount() == 1) {
                        final String errorMessage = String.format("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM, please split it into multiple smaller statements");
                        logger.warn(errorMessage);
                        rhc.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(errorMessage)
                                .statusAttributeException(t).create());
                    } else {
                        final String errorMessage = (t.getMessage() == null) ? t.toString() : t.getMessage();
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), t);
                        rhc.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(errorMessage)
                                .statusAttributeException(t).create());
                    }
                }
            }
            return null;
        });
    }

    public static void writeResultList(final Context context, final List<Object> resultList, final ResponseStatusCode statusCode) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        if (statusCode == ResponseStatusCode.SERVER_ERROR) {
            ResponseMessage.Builder builder = ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR);
            if (resultList.size() > 0) {
                builder.statusMessage((String) resultList.get(0));
            }
            ctx.writeAndFlush(builder.create());
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

    public static synchronized void applyStrategy(Traversal traversal, GaiaConfig config, GraphStoreService graphStore) {
        GraphTraversalStrategies traversalStrategies = new GraphTraversalStrategies(config, graphStore);
        boolean removeTagOn = config.getOptimizationStrategyFlag(GaiaConfig.REMOVE_TAG);
        boolean labelPathRequireOn = config.getOptimizationStrategyFlag(GaiaConfig.LABEL_PATH_REQUIREMENT);
        boolean propertyCacheOn = config.getOptimizationStrategyFlag(GaiaConfig.PROPERTY_CACHE);
        logger.debug("remove {}, require {}", removeTagOn, labelPathRequireOn);
        if (propertyCacheOn) {
            traversalStrategies.removeStrategies(PropertyShuffleStrategy.class);
            traversalStrategies.removeStrategies(OrderGuaranteeStrategy.class);
        } else {
            traversalStrategies.addStrategyByPriority(GraphTraversalStrategies.PROPERTY_SHUFFLE_PRIORITY, PropertyShuffleStrategy.instance());
            traversalStrategies.addStrategyByPriority(GraphTraversalStrategies.ORDER_GUARANTEE_PRIORITY, OrderGuaranteeStrategy.instance());
        }
        traversal.asAdmin().setStrategies(traversalStrategies);
        traversal.asAdmin().applyStrategies();
        if (removeTagOn || labelPathRequireOn) {
            PathHistoryStrategy.setIsPathRequireOn(labelPathRequireOn);
            PathHistoryStrategy.setIsRemoveTagOn(removeTagOn);
            PathHistoryStrategy.instance().apply(traversal.asAdmin());
        }
        if (propertyCacheOn) {
            PreCachePropertyStrategy.instance().apply(traversal.asAdmin());
        }
    }
}
