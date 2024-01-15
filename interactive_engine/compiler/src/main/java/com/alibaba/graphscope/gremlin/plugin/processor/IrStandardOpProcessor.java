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
import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.ir.tools.QueryIdGenerator;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.plugin.MetricsCollector;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrGremlinScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.script.GremlinCalciteScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.strategy.ExpandFusionStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.strategy.RemoveUselessStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.strategy.ScanFusionStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.gremlin.result.processor.AbstractResultProcessor;
import com.alibaba.graphscope.gremlin.result.processor.GremlinResultProcessor;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.script.SimpleBindings;

public class IrStandardOpProcessor extends StandardOpProcessor {
    protected final Graph graph;
    protected final GraphTraversalSource g;
    protected final Configs configs;
    /**
     * todo: replace with {@link com.alibaba.graphscope.common.client.ExecutionClient} after unifying Gremlin into the Calcite stack
     */
    protected final RpcClient rpcClient;

    protected final IrMetaQueryCallback metaQueryCallback;
    protected final QueryIdGenerator idGenerator;
    protected final QueryCache queryCache;
    protected final ExecutionClient executionClient;

    public IrStandardOpProcessor(
            Configs configs,
            QueryIdGenerator idGenerator,
            QueryCache queryCache,
            ExecutionClient executionClient,
            ChannelFetcher fetcher,
            IrMetaQueryCallback metaQueryCallback,
            Graph graph,
            GraphTraversalSource g) {
        this.graph = graph;
        this.g = g;
        this.configs = configs;
        this.rpcClient = new RpcClient(fetcher.fetch());
        this.metaQueryCallback = metaQueryCallback;
        this.idGenerator = idGenerator;
        this.queryCache = queryCache;
        this.executionClient = executionClient;
    }

    @Override
    protected void evalOpInternal(
            final Context ctx,
            final Supplier<GremlinExecutor> gremlinExecutorSupplier,
            final AbstractEvalOpProcessor.BindingSupplier bindingsSupplier) {
        RequestMessage msg = ctx.getRequestMessage();
        GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        Map<String, Object> args = msg.getArgs();
        String script = (String) args.get("gremlin");

        String defaultValidateQuery = "''";
        // ad-hoc handling for connection validation
        if (script.equals(defaultValidateQuery)) {
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).create());
            return;
        }
        long jobId = idGenerator.generateId();
        String jobName = idGenerator.generateName(jobId);
        IrMeta irMeta = metaQueryCallback.beforeExec();
        QueryStatusCallback statusCallback = createQueryStatusCallback(script, jobId);
        String language = FrontendConfig.GREMLIN_SCRIPT_LANGUAGE_NAME.get(configs);
        GremlinExecutor.LifeCycle lifeCycle;
        switch (language) {
            case AntlrGremlinScriptEngineFactory.LANGUAGE_NAME:
                lifeCycle =
                        createLifeCycle(
                                ctx,
                                gremlinExecutorSupplier,
                                bindingsSupplier,
                                irMeta,
                                statusCallback);
                break;
            case GremlinCalciteScriptEngineFactory.LANGUAGE_NAME:
                lifeCycle =
                        new LifeCycleSupplier(
                                        ctx,
                                        queryCache,
                                        executionClient,
                                        jobId,
                                        jobName,
                                        irMeta,
                                        statusCallback)
                                .get();
                break;
            default:
                throw new IllegalArgumentException("invalid script language name: " + language);
        }
        try {
            CompletableFuture<Object> evalFuture =
                    gremlinExecutor.eval(script, language, new SimpleBindings(), lifeCycle);
            evalFuture.handle(
                    (v, t) -> {
                        metaQueryCallback.afterExec(irMeta);
                        if (t != null) {
                            statusCallback.onEnd(false, null);
                            if (v instanceof AbstractResultProcessor) {
                                ((AbstractResultProcessor) v).cancel();
                            }
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
                                    statusCallback.getQueryLogger().warn(errorMessage);
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
                                    statusCallback.getQueryLogger().warn(errorMessage, t);
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
                                    statusCallback.getQueryLogger().warn(errorMessage);
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
                                    statusCallback
                                            .getQueryLogger()
                                            .warn(
                                                    String.format(
                                                            "Exception processing a script on"
                                                                    + " request [%s].",
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
            statusCallback.getQueryLogger().error(var17.getMessage());
            ctx.writeAndFlush(
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.TOO_MANY_REQUESTS)
                            .statusMessage(var17.getMessage())
                            .create());
        }
    }

    protected QueryStatusCallback createQueryStatusCallback(String query, long queryId) {
        return new QueryStatusCallback(
                new MetricsCollector(evalOpTimer), new QueryLogger(query, queryId));
    }

    protected GremlinExecutor.LifeCycle createLifeCycle(
            Context ctx,
            Supplier<GremlinExecutor> gremlinExecutorSupplier,
            BindingSupplier bindingsSupplier,
            IrMeta irMeta,
            QueryStatusCallback statusCallback) {
        QueryTimeoutConfig timeoutConfig = new QueryTimeoutConfig(ctx.getRequestTimeout());
        return GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(timeoutConfig.getExecutionTimeoutMS())
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
                                                    ctx, traversal, statusCallback, timeoutConfig),
                                            irMeta,
                                            timeoutConfig,
                                            statusCallback.getQueryLogger());
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .create();
    }

    // add script argument to print with ir plan
    protected void processTraversal(
            Traversal traversal,
            ResultProcessor resultProcessor,
            IrMeta irMeta,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger)
            throws InvalidProtocolBufferException, IOException, RuntimeException {
        // get configs per query from traversal
        Configs queryConfigs = getQueryConfigs(traversal);

        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        // fuse order with limit to topK
        InterOpCollection.applyStrategies(opCollection);
        // add sink operator
        InterOpCollection.process(opCollection);

        long jobId = queryLogger.getQueryId();
        IrPlan irPlan = new IrPlan(irMeta, opCollection);
        // print script and jobName with ir plan
        queryLogger.info("ir plan {}", irPlan.getPlanAsJson());
        byte[] physicalPlanBytes = irPlan.toPhysicalBytes(queryConfigs);
        irPlan.close();

        PegasusClient.JobRequest request =
                PegasusClient.JobRequest.newBuilder()
                        .setPlan(ByteString.copyFrom(physicalPlanBytes))
                        .build();
        String jobName = "ir_plan_" + jobId;
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(jobId)
                        .setJobName(jobName)
                        .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(queryConfigs))
                        .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(queryConfigs))
                        .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(queryConfigs))
                        .setBatchCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(queryConfigs))
                        .setTimeLimit(timeoutConfig.getEngineTimeoutMS())
                        .setAll(PegasusClient.Empty.newBuilder().build())
                        .build();
        request = request.toBuilder().setConf(jobConfig).build();

        this.rpcClient.submit(request, resultProcessor, timeoutConfig.getChannelTimeoutMS());
    }

    private Configs getQueryConfigs(Traversal traversal) {
        // the config priority is query > system env > system property > config file
        Iterator<Object> keyIterator = this.configs.getKeys();
        Map configMap = Maps.newHashMap();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next().toString();
            configMap.put(key, this.configs.get(key));
        }
        if (traversal instanceof IrCustomizedTraversal) {
            Optional<TraversalSource> sourceOpt =
                    ((IrCustomizedTraversal) traversal).getTraversalSource();
            if (sourceOpt.isPresent()) {
                // will override the config set before
                configMap.putAll(((IrCustomizedTraversalSource) sourceOpt.get()).getConfigs());
            }
        }
        return new Configs(configMap);
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
        // fuse outE() + hasLabel(..)
        strategies.add(InlineFilterStrategy.instance());
        strategies.add(ExpandFusionStepStrategy.instance());
        traversal.asAdmin().applyStrategies();
    }

    @Override
    public void close() throws Exception {
        if (this.rpcClient != null) {
            this.rpcClient.shutdown();
        }
    }
}
