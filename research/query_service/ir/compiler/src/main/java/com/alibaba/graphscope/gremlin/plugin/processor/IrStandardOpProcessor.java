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

package com.alibaba.graphscope.gremlin.plugin.processor;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.client.*;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.strategy.RemoveUselessStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.strategy.ScanFusionStepStrategy;
import com.alibaba.graphscope.gremlin.result.GremlinResultAnalyzer;
import com.alibaba.graphscope.gremlin.result.GremlinResultProcessor;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.script.Bindings;
import javax.script.SimpleBindings;

public class IrStandardOpProcessor extends StandardOpProcessor {
    private static Logger logger = LoggerFactory.getLogger(IrStandardOpProcessor.class);
    protected static final AtomicLong JOB_ID_COUNTER = new AtomicLong(0L);
    protected Graph graph;
    protected GraphTraversalSource g;
    protected Configs configs;
    protected RpcBroadcastProcessor broadcastProcessor;
    protected IrMetaFetcher irMetaFetcher;
    protected IrMetaQueryCallback metaQueryCallback;

    public IrStandardOpProcessor(
            Configs configs,
            IrMetaFetcher irMetaFetcher,
            RpcChannelFetcher fetcher,
            IrMetaQueryCallback metaQueryCallback,
            Graph graph,
            GraphTraversalSource g) {
        this.graph = graph;
        this.g = g;
        this.configs = configs;
        this.irMetaFetcher = irMetaFetcher;
        this.broadcastProcessor = new RpcBroadcastProcessor(fetcher);
        this.metaQueryCallback = metaQueryCallback;
    }

    @Override
    protected void evalOpInternal(
            final Context ctx,
            final Supplier<GremlinExecutor> gremlinExecutorSupplier,
            final AbstractEvalOpProcessor.BindingSupplier bindingsSupplier) {
        com.codahale.metrics.Timer.Context timerContext = evalOpTimer.time();
        RequestMessage msg = ctx.getRequestMessage();
        GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        Map<String, Object> args = msg.getArgs();
        String script = (String) args.get("gremlin");
        // replace with antlr parser
        String language = AntlrToJavaScriptEngineFactory.ENGINE_NAME;
        Bindings bindings = new SimpleBindings();

        GremlinExecutor.LifeCycle lifeCycle =
                createLifeCycle(ctx, gremlinExecutorSupplier, bindingsSupplier);

        try {
            CompletableFuture<Object> evalFuture =
                    gremlinExecutor.eval(script, language, bindings, lifeCycle);
            evalFuture.handle(
                    (v, t) -> {
                        long elapsed = timerContext.stop();
                        logger.info(
                                "query \"{}\" total execution time is {} ms",
                                script,
                                elapsed / 1000000.0f);
                        if (t != null) {
                            Optional<Throwable> possibleTemporaryException =
                                    determineIfTemporaryException(t);
                            if (possibleTemporaryException.isPresent()) {
                                ctx.writeAndFlush(
                                        ResponseMessage.build(msg)
                                                .code(ResponseStatusCode.SERVER_ERROR_TEMPORARY)
                                                .statusMessage(
                                                        ((Throwable)
                                                                        possibleTemporaryException
                                                                                .get())
                                                                .getMessage())
                                                .statusAttributeException(
                                                        (Throwable)
                                                                possibleTemporaryException.get())
                                                .create());
                            } else if (t instanceof OpProcessorException) {
                                ctx.writeAndFlush(((OpProcessorException) t).getResponseMessage());
                            } else {
                                String errorMessage;
                                if (t instanceof TimedInterruptTimeoutException) {
                                    errorMessage =
                                            String.format(
                                                    "A timeout occurred within the script during"
                                                            + " evaluation of [%s] - consider"
                                                            + " increasing the limit given to"
                                                            + " TimedInterruptCustomizerProvider",
                                                    msg);
                                    logger.warn(errorMessage);
                                    ctx.writeAndFlush(
                                            ResponseMessage.build(msg)
                                                    .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                    .statusMessage(
                                                            "Timeout during script evaluation"
                                                                + " triggered by"
                                                                + " TimedInterruptCustomizerProvider")
                                                    .statusAttributeException(t)
                                                    .create());
                                } else if (t instanceof TimeoutException) {
                                    errorMessage =
                                            String.format(
                                                    "Script evaluation exceeded the configured"
                                                            + " threshold for request [%s]",
                                                    msg);
                                    logger.warn(errorMessage, t);
                                    ctx.writeAndFlush(
                                            ResponseMessage.build(msg)
                                                    .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                    .statusMessage(t.getMessage())
                                                    .statusAttributeException(t)
                                                    .create());
                                } else if (t instanceof MultipleCompilationErrorsException
                                        && t.getMessage().contains("Method too large")
                                        && ((MultipleCompilationErrorsException) t)
                                                        .getErrorCollector()
                                                        .getErrorCount()
                                                == 1) {
                                    errorMessage =
                                            String.format(
                                                    "The Gremlin statement that was submitted"
                                                        + " exceeds the maximum compilation size"
                                                        + " allowed by the JVM, please split it"
                                                        + " into multiple smaller statements - %s",
                                                    msg);
                                    logger.warn(errorMessage);
                                    ctx.writeAndFlush(
                                            ResponseMessage.build(msg)
                                                    .code(
                                                            ResponseStatusCode
                                                                    .SERVER_ERROR_EVALUATION)
                                                    .statusMessage(errorMessage)
                                                    .statusAttributeException(t)
                                                    .create());
                                } else {
                                    errorMessage =
                                            t.getMessage() == null ? t.toString() : t.getMessage();
                                    logger.warn(
                                            String.format(
                                                    "Exception processing a script on request"
                                                            + " [%s].",
                                                    msg),
                                            t);
                                    ctx.writeAndFlush(
                                            ResponseMessage.build(msg)
                                                    .code(
                                                            ResponseStatusCode
                                                                    .SERVER_ERROR_EVALUATION)
                                                    .statusMessage(errorMessage)
                                                    .statusAttributeException(t)
                                                    .create());
                                }
                            }
                        }
                        return null;
                    });
        } catch (RejectedExecutionException var17) {
            ctx.writeAndFlush(
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.TOO_MANY_REQUESTS)
                            .statusMessage("Rate limiting")
                            .create());
        }
    }

    protected GremlinExecutor.LifeCycle createLifeCycle(
            Context ctx,
            Supplier<GremlinExecutor> gremlinExecutorSupplier,
            BindingSupplier bindingsSupplier) {
        final RequestMessage msg = ctx.getRequestMessage();
        final Settings settings = ctx.getSettings();
        final Map<String, Object> args = msg.getArgs();
        long seto =
                args.containsKey("evaluationTimeout")
                        ? ((Number) args.get("evaluationTimeout")).longValue()
                        : settings.getEvaluationTimeout();

        return GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(seto)
                .beforeEval(
                        b -> {
                            try {
                                b.putAll(bindingsSupplier.get());
                                b.put("graph", graph);
                                b.put("g", g);
                            } catch (OpProcessorException ope) {
                                throw new RuntimeException(ope);
                            }
                        })
                .transformResult(
                        o -> {
                            if (o != null && o instanceof Traversal) {
                                applyStrategies((Traversal) o);
                            }
                            return o;
                        })
                .withResult(
                        o -> {
                            try {
                                if (o != null && o instanceof Traversal) {
                                    Traversal traversal = (Traversal) o;
                                    processTraversal(
                                            traversal,
                                            new GremlinResultProcessor(
                                                    ctx, GremlinResultAnalyzer.analyze(traversal)));
                                }
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException(e);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .create();
    }

    protected void processTraversal(Traversal traversal, ResultProcessor resultProcessor)
            throws InvalidProtocolBufferException, IOException, RuntimeException {
        IrMeta irMeta = metaQueryCallback.beforeExec();

        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        // fuse order with limit to topK
        InterOpCollection.applyStrategies(opCollection);
        // add sink operator
        InterOpCollection.process(opCollection);

        IrPlan irPlan = new IrPlan(irMeta, opCollection);
        logger.info("{}", irPlan.getPlanAsJson());

        byte[] physicalPlanBytes = irPlan.toPhysicalBytes(configs);
        irPlan.close();

        long jobId = JOB_ID_COUNTER.incrementAndGet();
        String jobName = "ir_plan_" + jobId;

        PegasusClient.JobRequest request =
                PegasusClient.JobRequest.parseFrom(physicalPlanBytes);
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(jobId)
                        .setJobName(jobName)
                        .setWorkers(
                                PegasusConfig.PEGASUS_WORKER_NUM.get(
                                        configs))
                        .setBatchSize(
                                PegasusConfig.PEGASUS_BATCH_SIZE.get(
                                        configs))
                        .setMemoryLimit(
                                PegasusConfig.PEGASUS_MEMORY_LIMIT.get(
                                        configs))
                        .setBatchCapacity(
                                PegasusConfig.PEGASUS_OUTPUT_CAPACITY
                                        .get(configs))
                        .setTimeLimit(
                                PegasusConfig.PEGASUS_TIMEOUT.get(
                                        configs))
                        .setAll(PegasusClient.Empty.newBuilder().build())
                        .build();
        request = request.toBuilder().setConf(jobConfig).build();
        broadcastProcessor.broadcast(request, resultProcessor);

        metaQueryCallback.afterExec(irMeta);
    }

    public static void applyStrategies(Traversal traversal) {
        TraversalStrategies traversalStrategies = traversal.asAdmin().getStrategies();
        Set<TraversalStrategy<?>> strategies =
                Utils.getFieldValue(
                        DefaultTraversalStrategies.class,
                        traversalStrategies,
                        "traversalStrategies");
        strategies.clear();
        strategies.add(ScanFusionStepStrategy.instance());
        strategies.add(RemoveUselessStepStrategy.instance());
        traversal.asAdmin().applyStrategies();
    }
}
