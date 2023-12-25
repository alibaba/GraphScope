/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.integration.processor;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.ir.tools.QueryIdGenerator;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.integration.resultx.GremlinTestRecordParser;
import com.alibaba.graphscope.gremlin.integration.resultx.GremlinTestResultProcessor;
import com.alibaba.graphscope.gremlin.plugin.QueryStatusCallback;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrGremlinScriptEngine;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrGremlinScriptEngineFactory;
import com.alibaba.graphscope.gremlin.plugin.script.GremlinCalciteScriptEngineFactory;
import com.alibaba.graphscope.gremlin.resultx.ResultSchema;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

public class IrTestOpProcessor extends IrStandardOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    private AntlrGremlinScriptEngine scriptEngine;
    private ScriptContext context;
    private GraphProperties testGraph;

    public IrTestOpProcessor(
            Configs configs,
            QueryIdGenerator idGenerator,
            QueryCache queryCache,
            ExecutionClient executionClient,
            ChannelFetcher fetcher,
            IrMetaQueryCallback metaQueryCallback,
            Graph graph,
            GraphTraversalSource g,
            GraphProperties testGraph) {
        super(
                configs,
                idGenerator,
                queryCache,
                executionClient,
                fetcher,
                metaQueryCallback,
                graph,
                g);
        this.context = new SimpleScriptContext();
        Bindings globalBindings = new SimpleBindings();
        globalBindings.put("g", g);
        this.context.setBindings(globalBindings, ScriptContext.ENGINE_SCOPE);
        this.scriptEngine = new AntlrGremlinScriptEngine();
        this.testGraph = testGraph;
    }

    @Override
    public String getName() {
        return "traversal";
    }

    @Override
    public ThrowingConsumer<Context> select(Context ctx) {
        final RequestMessage message = ctx.getRequestMessage();
        final ThrowingConsumer<Context> op;
        logger.debug("tokens ops is {}", message.getOp());
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                op =
                        (context -> {
                            Bytecode byteCode =
                                    (Bytecode) message.getArgs().get(Tokens.ARGS_GREMLIN);
                            String script = getScript(byteCode);
                            long queryId = idGenerator.generateId();
                            String queryName = idGenerator.generateName(queryId);
                            IrMeta irMeta = metaQueryCallback.beforeExec();
                            QueryStatusCallback statusCallback =
                                    createQueryStatusCallback(script, queryId);
                            QueryTimeoutConfig timeoutConfig =
                                    new QueryTimeoutConfig(
                                            FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs));
                            String language =
                                    FrontendConfig.GREMLIN_SCRIPT_LANGUAGE_NAME.get(configs);
                            switch (language) {
                                case AntlrGremlinScriptEngineFactory.LANGUAGE_NAME:
                                    Traversal traversal =
                                            (Traversal) scriptEngine.eval(script, this.context);
                                    applyStrategies(traversal);
                                    processTraversal(
                                            traversal,
                                            new com.alibaba.graphscope.gremlin.integration.result
                                                    .GremlinTestResultProcessor(
                                                    ctx,
                                                    traversal,
                                                    statusCallback,
                                                    testGraph,
                                                    this.configs,
                                                    timeoutConfig),
                                            irMeta,
                                            new QueryTimeoutConfig(ctx.getRequestTimeout()),
                                            statusCallback.getQueryLogger());
                                    break;
                                case GremlinCalciteScriptEngineFactory.LANGUAGE_NAME:
                                    QueryCache.Value value =
                                            queryCache.get(queryCache.createKey(script, irMeta));
                                    GraphPlanner.Summary summary = value.summary;
                                    ResultSchema resultSchema =
                                            new ResultSchema(summary.getLogicalPlan());
                                    ExecutionResponseListener listener =
                                            new GremlinTestResultProcessor(
                                                    ctx,
                                                    statusCallback,
                                                    new GremlinTestRecordParser(
                                                            resultSchema,
                                                            testGraph.getProperties(configs)),
                                                    resultSchema);
                                    if (value.result != null && value.result.isCompleted) {
                                        List<IrResult.Results> records = value.result.records;
                                        records.forEach(k -> listener.onNext(k.getRecord()));
                                        listener.onCompleted();
                                    } else {
                                        executionClient.submit(
                                                new ExecutionRequest(
                                                        queryId,
                                                        queryName,
                                                        summary.getLogicalPlan(),
                                                        summary.getPhysicalPlan()),
                                                listener,
                                                timeoutConfig);
                                    }
                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                            "invalid script language name: " + language);
                            }
                            metaQueryCallback.afterExec(irMeta);
                        });
                return op;
            default:
                RequestMessage msg = ctx.getRequestMessage();
                String errorMsg = message.getOp() + " is unsupported";
                ctx.writeAndFlush(
                        ResponseMessage.build(msg)
                                .code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                                .statusMessage(errorMsg)
                                .create());
                return null;
        }
    }

    private String getScript(Bytecode byteCode) {
        String script = GroovyTranslator.of("g").translate(byteCode).getScript();
        // remove type cast from original script, g.V().has("age",P.gt((int) 30))
        List<String> typeCastStrs =
                Arrays.asList("\\(int\\)", "\\(long\\)", "\\(double\\)", "\\(boolean\\)");
        for (String type : typeCastStrs) {
            script = script.replaceAll(type, "");
        }
        return script;
    }
}
