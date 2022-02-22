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

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.client.ResultParser;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.integration.result.GremlinTestResultProcessor;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngine;
import com.alibaba.graphscope.gremlin.result.GremlinResultAnalyzer;
import com.alibaba.pegasus.service.protocol.PegasusClient;
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

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IrTestOpProcessor extends IrStandardOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    private AntlrToJavaScriptEngine scriptEngine;
    private ScriptContext context;

    public IrTestOpProcessor(Configs configs) {
        super(configs);
        context = new SimpleScriptContext();
        Bindings globalBindings = new SimpleBindings();
        globalBindings.put("g", g);
        context.setBindings(globalBindings, ScriptContext.ENGINE_SCOPE);
        scriptEngine = new AntlrToJavaScriptEngine();
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
                op = (context -> {
                    Bytecode byteCode = (Bytecode) message.getArgs().get(Tokens.ARGS_GREMLIN);
                    String script = getScript(byteCode);
                    Traversal traversal = (Traversal) scriptEngine.eval(script, this.context);

                    applyStrategies(traversal);

                    InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
                    IrPlan irPlan = opCollection.buildIrPlan();

                    byte[] physicalPlanBytes = irPlan.toPhysicalBytes(configs);
                    irPlan.close();

                    int serverNum = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
                    List<Long> servers = new ArrayList<>();
                    for (long i = 0; i < serverNum; ++i) {
                        servers.add(i);
                    }

                    long jobId = JOB_ID_COUNTER.incrementAndGet();
                    String jobName = "ir_plan_" + jobId;

                    PegasusClient.JobRequest request = PegasusClient.JobRequest.parseFrom(physicalPlanBytes);
                    PegasusClient.JobConfig jobConfig = PegasusClient.JobConfig.newBuilder()
                            .setJobId(jobId)
                            .setJobName(jobName)
                            .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(configs))
                            .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(configs))
                            .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs))
                            .setOutputCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs))
                            .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(configs))
                            .addAllServers(servers)
                            .build();
                    request = request.toBuilder().setConf(jobConfig).build();

                    ResultParser resultParser = GremlinResultAnalyzer.analyze(traversal);
                    broadcastProcessor.broadcast(request, new GremlinTestResultProcessor(ctx, resultParser));
                });
                return op;
            default:
                RequestMessage msg = ctx.getRequestMessage();
                String errorMsg = message.getOp() + " is unsupported";
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                        .statusMessage(errorMsg).create());
                return null;
        }
    }

    @Override
    public void close() throws Exception {
        this.broadcastProcessor.close();
    }

    private String getScript(Bytecode byteCode) {
        String script = GroovyTranslator.of("g").translate(byteCode);
        // remove type cast from original script, g.V().has("age",P.gt((int) 30))
        List<String> typeCastStrs = Arrays.asList("\\(int\\)", "\\(long\\)", "\\(double\\)", "\\(boolean\\)");
        for (String type : typeCastStrs) {
            script = script.replaceAll(type, "");
        }
        return script;
    }
}
