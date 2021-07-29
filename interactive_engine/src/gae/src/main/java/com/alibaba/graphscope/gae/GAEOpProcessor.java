package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.TraversalSourceGraph;
import com.alibaba.graphscope.gaia.processor.AbstractGraphOpProcessor;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkperpop.gremlin.groovy.custom.CustomGraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

public class GAEOpProcessor extends AbstractGraphOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GAEOpProcessor.class);
    private InstanceConfig instanceConfig;
    private Graph graph;
    private GraphTraversalSource traversalSource;

    public GAEOpProcessor(InstanceConfig instanceConfig) {
        super(null, null);
        this.instanceConfig = instanceConfig;
        this.graph = TraversalSourceGraph.open(new BaseConfiguration());
        this.traversalSource = graph.traversal(CustomGraphTraversalSource.class);
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
                        b.put("graph", this.graph);
                        b.put("g", this.traversalSource);
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
                        Map<String, Object> steps = processQuery((Traversal) o, config);
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

    public static Map<String, Object> processQuery(Traversal query, Map<String, Object> config) {
        if (QueryType.PAGE_RANK.isValid(query)) {
            return QueryType.PAGE_RANK.generate(config);
        } else if (QueryType.GRAPH_LEARN.isValid(query)) {
            return QueryType.GRAPH_LEARN.generate(config);
        } else if (QueryType.SSSP.isValid(query)) {
            return QueryType.SSSP.generate(config);
        } else {
            throw new UnsupportedOperationException("");
        }
    }
}
