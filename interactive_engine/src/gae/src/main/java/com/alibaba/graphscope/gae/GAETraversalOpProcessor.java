package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.TraversalSourceGraph;
import com.alibaba.graphscope.gaia.processor.AbstractGraphOpProcessor;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkperpop.gremlin.groovy.custom.CustomGraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.Map;

public class GAETraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GAETraversalOpProcessor.class);
    private InstanceConfig instanceConfig;
    private Graph graph;
    private GraphTraversalSource traversalSource;

    public GAETraversalOpProcessor(InstanceConfig instanceConfig) {
        super(false);
        this.instanceConfig = instanceConfig;
        this.graph = TraversalSourceGraph.open(new BaseConfiguration());
        this.traversalSource = graph.traversal(CustomGraphTraversalSource.class);
    }

    @Override
    public String getName() {
        return "gae_traversal";
    }

    @Override
    public ThrowingConsumer<Context> select(Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        final ThrowingConsumer<Context> op;
        final GremlinExecutor executor = ctx.getGremlinExecutor();
        final SimpleBindings b = new SimpleBindings();
        b.put("graph", this.graph);
        b.put("g", this.traversalSource);
        final Map<String, String> aliases = (Map<String, String>) message.optionalArgs(Tokens.ARGS_ALIASES).get();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        logger.info("tokens ops is {}", message.getOp());
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                op = (context -> {
                    Bytecode byteCode = (Bytecode) message.getArgs().get(Tokens.ARGS_GREMLIN);
                    b.putAll(byteCode.getBindings());
                    final String str = GroovyTranslator.of("g").translate(byteCode);
                    final String transformed_script = str.replaceAll("__\\.", "");
                    Traversal traversal = (Traversal.Admin) executor.getScriptEngineManager().getEngineByName("gremlin-groovy").eval(transformed_script, b);

                    Map<String, Object> config = ImmutableMap.of(
                            "graph", instanceConfig.getGraphName(),
                            "traversal", traversal);
                    Map<String, Object> steps = GAEOpProcessor.processQuery(traversal, config);
                    String json = JsonUtils.toJson(steps);
                    AbstractGraphOpProcessor.writeResultList(ctx, Collections.singletonList(new DefaultRemoteTraverser(json, 1)), ResponseStatusCode.SUCCESS);
                });
                return op;
            default:
                String errorMsg = "not support " + message.getOp();
                GaiaGraphOpProcessor.writeResultList(ctx, Collections.singletonList(errorMsg), ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS);
                return null;
        }
    }

    @Override
    public void close() throws Exception {
    }
}
