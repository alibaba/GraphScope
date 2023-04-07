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
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleWrapper;
import com.alibaba.graphscope.common.ir.runtime.LogicalPlanConverter;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiLogicalPlan;
import com.alibaba.graphscope.common.ir.runtime.ffi.RelToFfiConverter;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalPlan;
import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.schema.StatisticSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrCypherScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrGremlinScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.strategy.ExpandFusionStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.strategy.RemoveUselessStepStrategy;
import com.alibaba.graphscope.gremlin.plugin.strategy.ScanFusionStepStrategy;
import com.alibaba.graphscope.gremlin.result.processor.CypherResultProcessor;
import com.alibaba.graphscope.gremlin.result.processor.GremlinResultProcessor;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.jna.Pointer;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexBuilder;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
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
import java.util.function.Function;
import java.util.function.Supplier;

import javax.script.SimpleBindings;

public class IrStandardOpProcessor extends StandardOpProcessor {
    private static Logger metricLogger = LoggerFactory.getLogger("MetricLog");
    private static Logger logger = LoggerFactory.getLogger(IrStandardOpProcessor.class);

    protected static final AtomicLong JOB_ID_COUNTER = new AtomicLong(0L);
    protected Graph graph;
    protected GraphTraversalSource g;
    protected Configs configs;
    protected PlannerConfig plannerConfig;
    protected RpcBroadcastProcessor broadcastProcessor;
    protected IrMetaFetcher irMetaFetcher;
    protected IrMetaQueryCallback metaQueryCallback;

    protected final Function<StatisticSchema, GraphBuilder> graphBuilderGenerator;

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
        this.plannerConfig = PlannerConfig.create(this.configs);
        this.irMetaFetcher = irMetaFetcher;
        this.broadcastProcessor = new RpcBroadcastProcessor(fetcher);
        this.metaQueryCallback = metaQueryCallback;

        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
        this.graphBuilderGenerator =
                (StatisticSchema schema) -> {
                    Objects.requireNonNull(schema);
                    GraphOptCluster optCluster =
                            GraphOptCluster.create(getRelOptPlanner(), rexBuilder);
                    return GraphBuilder.create(
                            null, optCluster, new GraphOptSchema(optCluster, schema));
                };
    }

    @Override
    protected void evalOpInternal(
            final Context ctx,
            final Supplier<GremlinExecutor> gremlinExecutorSupplier,
            final AbstractEvalOpProcessor.BindingSupplier bindingsSupplier) {
        long startTime = System.currentTimeMillis();
        com.codahale.metrics.Timer.Context timerContext = evalOpTimer.time();
        RequestMessage msg = ctx.getRequestMessage();
        GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        Map<String, Object> args = msg.getArgs();
        String script = (String) args.get("gremlin");

        // replace with antlr parser
        String language = getLanguageFromRequest(msg);

        long jobId = JOB_ID_COUNTER.incrementAndGet();
        IrMeta irMeta = metaQueryCallback.beforeExec();
        GremlinExecutor.LifeCycle lifeCycle =
                createLifeCycle(
                        ctx, gremlinExecutorSupplier, bindingsSupplier, jobId, script, irMeta);
        try {
            CompletableFuture<Object> evalFuture =
                    gremlinExecutor.eval(script, language, new SimpleBindings(), lifeCycle);
            evalFuture.handle(
                    (v, t) -> {
                        metaQueryCallback.afterExec(irMeta);
                        long elapsed = timerContext.stop();
                        logger.info(
                                "query \"{}\" total execution time is {} ms",
                                script,
                                elapsed / 1000000.0f);
                        boolean isSuccess = (t == null);
                        metricLogger.info(
                                "{} | {} | {} | {} | {}",
                                jobId,
                                script,
                                isSuccess,
                                elapsed / 1000000.0f,
                                startTime);
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

    protected String getLanguageFromRequest(RequestMessage msg) {
        Map<String, Object> args = msg.getArgs();
        if (args.containsKey("bindings")) {
            Map<String, Object> bindings = (Map<String, Object>) args.get("bindings");
            if (bindings.containsKey("language")) {
                return (String) bindings.get("language");
            }
        }
        // hack ways to get language opt from gremlin console, for this is the only remote
        // configurations can be set in the console
        if (args.containsKey("aliases")) {
            Map<String, String> aliases = (Map<String, String>) args.get("aliases");
            for (Map.Entry<String, String> alias : aliases.entrySet()) {
                if (alias.getValue().equals("graph")) {
                    return alias.getKey();
                }
            }
        }
        return AntlrGremlinScriptEngineFactory.LANGUAGE_NAME;
    }

    protected GremlinExecutor.LifeCycle createLifeCycle(
            Context ctx,
            Supplier<GremlinExecutor> gremlinExecutorSupplier,
            BindingSupplier bindingsSupplier,
            long jobId,
            String script,
            IrMeta irMeta) {
        final RequestMessage msg = ctx.getRequestMessage();
        final Settings settings = ctx.getSettings();
        final Map<String, Object> args = msg.getArgs();
        long seto =
                args.containsKey("evaluationTimeout")
                        ? ((Number) args.get("evaluationTimeout")).longValue()
                        : settings.getEvaluationTimeout();
        // replace with antlr parser
        String language = getLanguageFromRequest(msg);
        return GremlinExecutor.LifeCycle.build()
                .evaluationTimeoutOverride(seto)
                .beforeEval(
                        b -> {
                            try {
                                b.putAll(bindingsSupplier.get());
                                b.put("graph", graph);
                                b.put("g", g);
                                if (language.equals(
                                        AntlrGremlinScriptEngineFactory.LANGUAGE_NAME)) {
                                    // todo: prepare configs to parse gremlin
                                } else if (language.equals(
                                        AntlrCypherScriptEngineFactory.LANGUAGE_NAME)) {
                                    b.put(
                                            "graph.builder",
                                            graphBuilderGenerator.apply(irMeta.getSchema()));
                                }
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
                                            new GremlinResultProcessor(ctx, traversal),
                                            jobId,
                                            script,
                                            irMeta);
                                } else if (o != null && o instanceof GraphBuilder) {
                                    GraphBuilder builder = (GraphBuilder) o;
                                    GraphOptCluster optCluster =
                                            (GraphOptCluster) builder.getCluster();
                                    RelNode topNode = builder.build();
                                    // apply optimizations
                                    if (this.plannerConfig.isOn()) {
                                        RelOptPlanner planner = optCluster.getPlanner();
                                        planner.setRoot(topNode);
                                        topNode = planner.findBestExp();
                                    }
                                    if (language.equals(
                                            AntlrGremlinScriptEngineFactory.LANGUAGE_NAME)) {
                                        // todo: handle gremlin results
                                    } else if (language.equals(
                                            AntlrCypherScriptEngineFactory.LANGUAGE_NAME)) {
                                        processRelNode(
                                                topNode,
                                                optCluster,
                                                new CypherResultProcessor(ctx, topNode),
                                                jobId,
                                                script,
                                                irMeta,
                                                ctx);
                                    }
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
            long jobId,
            String script,
            IrMeta irMeta)
            throws InvalidProtocolBufferException, IOException, RuntimeException {
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        // fuse order with limit to topK
        InterOpCollection.applyStrategies(opCollection);
        // add sink operator
        InterOpCollection.process(opCollection);

        String jobName = "ir_plan_" + jobId;
        IrPlan irPlan = new IrPlan(irMeta, opCollection);
        // print script and jobName with ir plan
        logger.info(
                "gremlin query \"{}\", job conf name \"{}\", ir plan {}",
                script,
                jobName,
                irPlan.getPlanAsJson());
        byte[] physicalPlanBytes = irPlan.toPhysicalBytes(configs);
        irPlan.close();

        PegasusClient.JobRequest request = PegasusClient.JobRequest.parseFrom(physicalPlanBytes);
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(jobId)
                        .setJobName(jobName)
                        .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(configs))
                        .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(configs))
                        .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs))
                        .setBatchCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs))
                        .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(configs))
                        .setAll(PegasusClient.Empty.newBuilder().build())
                        .build();
        request = request.toBuilder().setConf(jobConfig).build();
        broadcastProcessor.broadcast(request, resultProcessor);
    }

    protected void processRelNode(
            RelNode topNode,
            GraphOptCluster optCluster,
            ResultProcessor resultProcessor,
            long jobId,
            String script,
            IrMeta irMeta,
            Context ctx)
            throws Exception {
        try (LogicalPlan<Pointer, byte[]> logicalPlan =
                new LogicalPlanConverter<>(
                                new GraphRelShuttleWrapper(
                                        new RelToFfiConverter(irMeta.getSchema().isColumnId())),
                                new FfiLogicalPlan(optCluster, irMeta, getPlanHints(irMeta)))
                        .go(topNode)) {
            String jobName = "ir_plan_" + jobId;
            if (logicalPlan.isReturnEmpty()) {
                logger.info(
                        "gremlin query \"{}\", job conf name \"{}\", relNode plan\n {}",
                        script,
                        jobName,
                        topNode.explain());
                // return empty results to the client
                RequestMessage msg = ctx.getRequestMessage();
                ctx.writeAndFlush(
                        ResponseMessage.build(msg).code(ResponseStatusCode.NO_CONTENT).create());
            } else {
                byte[] physicalPlanBytes = logicalPlan.toPhysical();
                // print script and jobName with ir plan
                logger.info(
                        "gremlin query \"{}\", job conf name \"{}\", ir plan {}",
                        script,
                        jobName,
                        logicalPlan.explain());
                PegasusClient.JobRequest request =
                        PegasusClient.JobRequest.parseFrom(physicalPlanBytes);
                PegasusClient.JobConfig jobConfig =
                        PegasusClient.JobConfig.newBuilder()
                                .setJobId(jobId)
                                .setJobName(jobName)
                                .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(configs))
                                .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(configs))
                                .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs))
                                .setBatchCapacity(
                                        PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs))
                                .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(configs))
                                .setAll(PegasusClient.Empty.newBuilder().build())
                                .build();
                request = request.toBuilder().setConf(jobConfig).build();
                broadcastProcessor.broadcast(request, resultProcessor);
            }
        }
    }

    protected List<RelHint> getPlanHints(IrMeta irMeta) {
        int servers = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
        int workers = PegasusConfig.PEGASUS_WORKER_NUM.get(configs);
        return ImmutableList.of(
                RelHint.builder("plan")
                        .hintOption("servers", String.valueOf(servers))
                        .hintOption("workers", String.valueOf(workers))
                        .hintOption("isColumnId", String.valueOf(irMeta.getSchema().isColumnId()))
                        .build());
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

    private RelOptPlanner getRelOptPlanner() {
        if (this.plannerConfig.isOn()) {
            PlannerConfig.Opt opt = this.plannerConfig.getOpt();
            switch (opt) {
                case RBO:
                    HepProgramBuilder hepBuilder = HepProgram.builder();
                    this.plannerConfig
                            .getRules()
                            .forEach(
                                    k -> {
                                        if (k.equals(FilterMatchRule.class.getSimpleName())) {
                                            hepBuilder.addRuleInstance(
                                                    FilterMatchRule.Config.DEFAULT.toRule());
                                        } else {
                                            // todo: add more rules
                                        }
                                    });
                    return new HepPlanner(hepBuilder.build());
                case CBO:
                default:
                    throw new UnsupportedOperationException(
                            "planner type " + opt.name() + " is unsupported yet");
            }
        } else {
            // return HepPlanner with empty rules if optimization is turned off
            return new HepPlanner(HepProgram.builder().build());
        }
    }
}
