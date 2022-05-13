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

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.integration.result.GremlinTestResultProcessor;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngine;
import com.alibaba.graphscope.gremlin.result.GremlinResultAnalyzer;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor;
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
    private AntlrToJavaScriptEngine scriptEngine;
    private ScriptContext context;
    private GraphProperties testGraph;

    public IrTestOpProcessor(
            Configs configs,
            IrMetaFetcher irMetaFetcher,
            RpcChannelFetcher fetcher,
            GraphProperties testGraph) {
        super(configs, irMetaFetcher, fetcher);
        this.context = new SimpleScriptContext();
        Bindings globalBindings = new SimpleBindings();
        globalBindings.put("g", g);
        this.context.setBindings(globalBindings, ScriptContext.ENGINE_SCOPE);
        this.scriptEngine = new AntlrToJavaScriptEngine();
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
                            Traversal traversal =
                                    (Traversal) scriptEngine.eval(script, this.context);

                            applyStrategies(traversal);

                            processTraversal(traversal,
                                    new GremlinTestResultProcessor(ctx, GremlinResultAnalyzer.analyze(traversal), testGraph));
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

    @Override
    public void close() throws Exception {
        this.broadcastProcessor.close();
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
