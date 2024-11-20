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
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.ir.tools.QueryIdGenerator;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.utils.ClassUtils;
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
import com.alibaba.graphscope.proto.frontend.Code;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.IdGenerator;

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

import java.math.BigInteger;
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
    protected final GraphPlanner graphPlanner;
    protected final ExecutionClient executionClient;
    protected Tracer tracer;
    protected LongHistogram queryHistogram;
    /**
     * for success query
     * Print if the threshold is exceeded
     */
    protected long printThreshold;

    protected IdGenerator opentelemetryIdGenerator;

    public IrStandardOpProcessor(
            Configs configs,
            QueryIdGenerator idGenerator,
            QueryCache queryCache,
            GraphPlanner graphPlanner,
            ExecutionClient executionClient,
            ChannelFetcher fetcher,
            IrMetaQueryCallback metaQueryCallback,
            Graph graph,
            GraphTraversalSource g) {
        this.graph = graph;
        this.g = g;
        this.configs = configs;
        // hack implementation here: rpc client is the old way to submit job (gremlin -> traversal
        // -> ir_core -> pegasus), we should remove it after replacing it with gremlin-calcite
        // stack.
        if (FrontendConfig.ENGINE_TYPE.get(this.configs).equals("pegasus")) {
            this.rpcClient = new RpcClient(fetcher.fetch());
        } else {
            this.rpcClient = null;
        }
        this.metaQueryCallback = metaQueryCallback;
        this.idGenerator = idGenerator;
        this.queryCache = queryCache;
        this.graphPlanner = graphPlanner;
        this.executionClient = executionClient;
        this.printThreshold = FrontendConfig.QUERY_PRINT_THRESHOLD_MS.get(configs);
        this.opentelemetryIdGenerator = IdGenerator.random();
        initTracer();
        initMetrics();
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
        Map<String, Object> bindings =
                args.get("bindings") == null ? null : (Map<String, Object>) args.get("bindings");
        String upTraceId =
                (bindings == null || bindings.get("X-Trace-ID") == null)
                        ? null
                        : String.valueOf(bindings.get("X-Trace-ID"));

        String defaultValidateQuery = "''";
        // ad-hoc handling for connection validation
        if (script.equals(defaultValidateQuery)) {
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).create());
            return;
        }
        BigInteger jobId = idGenerator.generateId();
        String jobName = idGenerator.generateName(jobId);
        String language = FrontendConfig.GREMLIN_SCRIPT_LANGUAGE_NAME.get(configs);
        IrMeta irMeta =
                ClassUtils.callExceptionWithDetails(
                        () -> metaQueryCallback.beforeExec(),
                        Code.META_SCHEMA_NOT_READY,
                        Map.of("QueryId", jobId));
        // If the current graph schema is empty (as service startup can occur before data loading in
        // Groot), we temporarily switch to the original IR core.
        // In the future, once schema-free support is implemented, we will replace this temporary
        // solution.
        if (irMeta.getSchema().getVertexList().isEmpty()
                && irMeta.getSchema().getEdgeList().isEmpty()) {
            language = AntlrGremlinScriptEngineFactory.LANGUAGE_NAME;
        }
        QueryStatusCallback statusCallback =
                ClassUtils.createQueryStatusCallback(
                        jobId,
                        upTraceId,
                        script,
                        new MetricsCollector.Gremlin(evalOpTimer),
                        queryHistogram,
                        configs);
        statusCallback
                .getQueryLogger()
                .info("[query][received]: query received from the gremlin client");
        QueryTimeoutConfig timeoutConfig = new QueryTimeoutConfig(ctx.getRequestTimeout());
        GremlinExecutor.LifeCycle lifeCycle;
        switch (language) {
            case AntlrGremlinScriptEngineFactory.LANGUAGE_NAME:
                lifeCycle =
                        createLifeCycle(
                                ctx,
                                gremlinExecutorSupplier,
                                bindingsSupplier,
                                irMeta,
                                statusCallback,
                                timeoutConfig);
                break;
            case GremlinCalciteScriptEngineFactory.LANGUAGE_NAME:
                lifeCycle =
                        new LifeCycleSupplier(
                                        configs,
                                        ctx,
                                        queryCache,
                                        graphPlanner,
                                        executionClient,
                                        jobId,
                                        jobName,
                                        irMeta,
                                        statusCallback,
                                        timeoutConfig)
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
                        if (t instanceof FrontendException) {
                            ((FrontendException) t).getDetails().put("QueryId", jobId);
                        }
                        // TimeoutException has been handled in ResultProcessor, skip it here
                        if (t != null && !(t instanceof TimeoutException)) {
                            statusCallback.onErrorEnd(t.getMessage());
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
            statusCallback.getQueryLogger().error(var17);
            ctx.writeAndFlush(
                    ResponseMessage.build(msg)
                            .code(ResponseStatusCode.TOO_MANY_REQUESTS)
                            .statusMessage(var17.getMessage())
                            .create());
        }
    }

    protected GremlinExecutor.LifeCycle createLifeCycle(
            Context ctx,
            Supplier<GremlinExecutor> gremlinExecutorSupplier,
            BindingSupplier bindingsSupplier,
            IrMeta irMeta,
            QueryStatusCallback statusCallback,
            QueryTimeoutConfig timeoutConfig) {
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
                            statusCallback
                                    .getQueryLogger()
                                    .info("[query][compiled]: traversal compiled");
                            return o;
                        })
                .withResult(
                        o -> {
                            if (o != null && o instanceof Traversal) {
                                Traversal traversal = (Traversal) o;
                                processTraversal(
                                        traversal,
                                        new GremlinResultProcessor(
                                                configs,
                                                ctx,
                                                traversal,
                                                statusCallback,
                                                timeoutConfig),
                                        irMeta,
                                        timeoutConfig,
                                        statusCallback.getQueryLogger());
                            }
                        })
                .create();
    }

    // add script argument to print with ir plan
    protected void processTraversal(
            Traversal traversal,
            AbstractResultProcessor resultProcessor,
            IrMeta irMeta,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger) {
        // get configs per query from traversal
        Configs queryConfigs = getQueryConfigs(traversal);

        InterOpCollection logicalPlan =
                ClassUtils.callException(
                        () -> {
                            InterOpCollection opCollection =
                                    (new InterOpCollectionBuilder(traversal)).build();
                            // fuse order with limit to topK
                            InterOpCollection.applyStrategies(opCollection);
                            // add sink operator
                            InterOpCollection.process(opCollection);
                            return opCollection;
                        },
                        Code.LOGICAL_PLAN_BUILD_FAILED);
        queryLogger.info("[query][compiled]: logical IR compiled");
        StringBuilder irPlanStr = new StringBuilder();
        PegasusClient.JobRequest physicalRequest =
                ClassUtils.callException(
                        () -> {
                            IrPlan irPlan = new IrPlan(irMeta, logicalPlan);
                            // print script and jobName with ir plan
                            queryLogger.info("Submitted query");
                            // Too verbose, since all identical queries produce identical plans,
                            // it's no need to print
                            // every plan in production.de
                            irPlanStr.append(irPlan.getPlanAsJson());
                            queryLogger.debug("ir plan {}", irPlanStr.toString());
                            queryLogger.setIrPlan(irPlanStr.toString());
                            byte[] physicalPlanBytes = irPlan.toPhysicalBytes(queryConfigs);
                            irPlan.close();
                            BigInteger jobId = queryLogger.getQueryId();
                            PegasusClient.JobRequest request =
                                    PegasusClient.JobRequest.newBuilder()
                                            .setPlan(ByteString.copyFrom(physicalPlanBytes))
                                            .build();
                            String jobName = "ir_plan_" + jobId;
                            PegasusClient.JobConfig jobConfig =
                                    PegasusClient.JobConfig.newBuilder()
                                            .setJobId(jobId.longValue())
                                            .setJobName(jobName)
                                            .setWorkers(
                                                    PegasusConfig.PEGASUS_WORKER_NUM.get(
                                                            queryConfigs))
                                            .setBatchSize(
                                                    PegasusConfig.PEGASUS_BATCH_SIZE.get(
                                                            queryConfigs))
                                            .setMemoryLimit(
                                                    PegasusConfig.PEGASUS_MEMORY_LIMIT.get(
                                                            queryConfigs))
                                            .setBatchCapacity(
                                                    PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(
                                                            queryConfigs))
                                            .setTimeLimit(timeoutConfig.getEngineTimeoutMS())
                                            .setAll(PegasusClient.Empty.newBuilder().build())
                                            .build();
                            request = request.toBuilder().setConf(jobConfig).build();
                            return request;
                        },
                        Code.PHYSICAL_PLAN_BUILD_FAILED);
        queryLogger.info("[query][compiled]: physical IR compiled");
        Span outgoing;
        // if exist up trace, useUpTraceId as current traceId
        if (TraceId.isValid(queryLogger.getUpstreamId())) {
            SpanContext spanContext =
                    SpanContext.createFromRemoteParent(
                            queryLogger.getUpstreamId(),
                            this.opentelemetryIdGenerator.generateSpanId(),
                            TraceFlags.getDefault(),
                            TraceState.getDefault());
            outgoing =
                    tracer.spanBuilder("frontend/query")
                            .setParent(
                                    io.opentelemetry.context.Context.current()
                                            .with(Span.wrap(spanContext)))
                            .setSpanKind(SpanKind.CLIENT)
                            .startSpan();
        } else {
            outgoing =
                    tracer.spanBuilder("frontend/query").setSpanKind(SpanKind.CLIENT).startSpan();
        }
        try (Scope ignored = outgoing.makeCurrent()) {
            outgoing.setAttribute("query.id", queryLogger.getQueryId().toString());
            outgoing.setAttribute("query.statement", queryLogger.getQuery());
            outgoing.setAttribute("query.plan", irPlanStr.toString());
            this.rpcClient.submit(
                    physicalRequest, resultProcessor, timeoutConfig.getChannelTimeoutMS());
            queryLogger.info("[query][submitted]: physical IR submitted");
            // request results from remote engine service in blocking way
            resultProcessor.request();
        } catch (Throwable t) {
            outgoing.setStatus(StatusCode.ERROR, "Submit failed!");
            outgoing.recordException(t);
            throw t;
        } finally {
            outgoing.end();
        }
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

    public void initTracer() {
        this.tracer = GlobalOpenTelemetry.getTracer("default");
    }

    public void initMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter("default");
        this.queryHistogram =
                meter.histogramBuilder("groot.frontend.query.duration")
                        .setDescription("Duration of gremlin queries.")
                        .setUnit("ms")
                        .ofLongs()
                        .build();
    }
}
