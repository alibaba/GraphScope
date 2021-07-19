package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.processor.AbstractGraphOpProcessor;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

public class GAEGremlinProcessor extends AbstractGraphOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GAEGremlinProcessor.class);
    private InstanceConfig instanceConfig;
    private GAEStepChainMaker stepChainMaker;

    public GAEGremlinProcessor(InstanceConfig instanceConfig) {
        super(null, null);
        this.instanceConfig = instanceConfig;
        this.stepChainMaker = new GAEStepChainMaker();
    }

    @Override
    public String getName() {
        return "gae";
    }

    @Override
    protected GremlinExecutor.LifeCycle createLifeCycle(Context ctx, Supplier<GremlinExecutor> gremlinExecutorSupplier, BindingSupplier bindingsSupplier) {
        final RequestMessage msg = ctx.getRequestMessage();
        final Settings settings = ctx.getSettings();
        final Map<String, Object> args = msg.getArgs();
        final long seto = args.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT)).longValue() : settings.scriptEvaluationTimeout;
        return GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(seto)
                .beforeEval(b -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                    } catch (OpProcessorException ope) {
                        throw new RuntimeException(ope);
                    }
                })
                .transformResult(o -> {
                    return o;
                })
                .withResult(o -> {
                    Map<String, Object> config = ImmutableMap.of(
                            "graph", instanceConfig.getGraphName(),
                            "traversal", (Traversal) o);
                    if (o != null && o instanceof Traversal) {
                        Map<String, Object> steps = stepChainMaker.generate(config);
                        String json = JsonUtils.toJson(steps);
                        writeResultList(ctx, Collections.singletonList(json), ResponseStatusCode.SUCCESS);
                    } else {
                        List<Object> results = new ArrayList<>();
                        if (o != null) {
                            results.add(o);
                        }
                        writeResultList(ctx, results, ResponseStatusCode.SUCCESS);
                    }
                }).create();
    }

    @Override
    public void close() throws Exception {

    }
}
