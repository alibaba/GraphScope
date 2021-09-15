package com.alibaba.graphscope.gaia.processor;

import com.alibaba.graphscope.gaia.broadcast.AbstractBroadcastProcessor;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.idmaker.IncrementalQueryIdMaker;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import com.alibaba.graphscope.gaia.result.GremlinResultProcessor;
import com.alibaba.graphscope.gaia.result.RemoteTraverserResultParser;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.graphscope.gaia.store.SchemaNotFoundException;
import com.alibaba.pegasus.builder.AbstractBuilder;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.Map;

public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    protected IdMaker queryIdMaker;
    protected AbstractBroadcastProcessor broadcastProcessor;
    private GaiaConfig config;
    private GraphStoreService graphStore;

    public TraversalOpProcessor(GaiaConfig config, GraphStoreService graphStore, AbstractBroadcastProcessor broadcastProcessor) {
        super(false);
        this.config = config;
        this.graphStore = graphStore;
        this.queryIdMaker = new IncrementalQueryIdMaker();
        this.broadcastProcessor = broadcastProcessor;
    }

    @Override
    public String getName() {
        return "traversal";
    }

    @Override
    public ThrowingConsumer<Context> select(Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        final ThrowingConsumer<Context> op;
        final GremlinExecutor executor = ctx.getGremlinExecutor();
        final SimpleBindings b = new SimpleBindings();
        final Map<String, String> aliases = (Map<String, String>) message.optionalArgs(Tokens.ARGS_ALIASES).get();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        logger.info("tokens ops is {}", message.getOp());
        if (config.getGraphType() == GraphType.MAXGRAPH) {
            graphStore.updateSnapShotId();
        }
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                op = (context -> {
                    try {
                        Object byteCode = message.getArgs().get(Tokens.ARGS_GREMLIN);
                        Traversal traversal = executor.eval((Bytecode) byteCode, new SimpleBindings(), null, traversalSourceName);
                        GaiaGraphOpProcessor.applyStrategy(traversal, config, graphStore);
                        long queryId = (long) queryIdMaker.getId(traversal);
                        TraversalBuilder traversalBuilder = new TraversalBuilder((Traversal.Admin) traversal)
                                .addConfig(PlanConfig.QUERY_ID, queryId)
                                .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker((Traversal.Admin) traversal))
                                .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId, config));
                        if (config.getGraphType() == GraphType.MAXGRAPH) {
                            traversalBuilder.addConfig(PlanConfig.SNAPSHOT_ID, Long.valueOf(graphStore.getSnapShotId()));
                        }
                        AbstractBuilder jobReqBuilder = new TraversalTranslator(traversalBuilder).translate();
                        PlanUtils.print(jobReqBuilder);
                        broadcastProcessor.broadcast(jobReqBuilder.build(),
                                new GremlinResultProcessor(ctx, new RemoteTraverserResultParser(traversalBuilder, graphStore, config)));
                        logger.info("query-{} finish", queryId);
                    } catch (SchemaNotFoundException e) {
                        throw new OpProcessorException("schema not found error",
                                ResponseMessage.build(message).code(ResponseStatusCode.SUCCESS).result(Collections.EMPTY_LIST).create());
                    }
                });
                return op;
            case Tokens.OPS_KEYS:
                GaiaGraphOpProcessor.writeResultList(ctx, Collections.EMPTY_LIST, ResponseStatusCode.SUCCESS);
                return null;
            default:
                String errorMsg = "not support " + message.getOp();
                GaiaGraphOpProcessor.writeResultList(ctx, Collections.singletonList(errorMsg), ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS);
                return null;
        }
    }

    @Override
    public void close() throws Exception {
        this.broadcastProcessor.close();
    }
}
