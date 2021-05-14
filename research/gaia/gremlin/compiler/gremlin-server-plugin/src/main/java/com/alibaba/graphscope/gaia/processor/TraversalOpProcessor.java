package com.alibaba.graphscope.gaia.processor;

import com.alibaba.graphscope.gaia.GlobalEngineConf;
import com.alibaba.graphscope.gaia.broadcast.AbstractBroadcastProcessor;
import com.alibaba.graphscope.gaia.broadcast.RpcBroadcastProcessor;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.idmaker.IncrementalQueryIdMaker;
import com.alibaba.graphscope.gaia.idmaker.TagIdMaker;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import com.alibaba.graphscope.gaia.result.DefaultResultParser;
import com.alibaba.graphscope.gaia.result.GremlinResultProcessor;
import com.alibaba.pegasus.builder.AbstractBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
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
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    protected IdMaker queryIdMaker;
    protected AbstractBroadcastProcessor broadcastProcessor;

    public TraversalOpProcessor() {
        super(false);
        this.queryIdMaker = new IncrementalQueryIdMaker();
        this.broadcastProcessor = new RpcBroadcastProcessor((List) GlobalEngineConf.getDefaultSysConf().get("hosts"));
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
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                op = (context -> {
                    Object byteCode = message.getArgs().get(Tokens.ARGS_GREMLIN);
                    Traversal traversal = executor.eval((Bytecode) byteCode, new SimpleBindings(), null, traversalSourceName);
                    MaxGraphOpProcessor.applyStrategy(traversal);
                    long queryId = (long) queryIdMaker.getId(traversal);
                    TraversalBuilder traversalBuilder = new TraversalBuilder((Traversal.Admin) traversal)
                            .addConfig(PlanConfig.QUERY_ID, queryId)
                            .addConfig(PlanConfig.TAG_ID_MAKER, new TagIdMaker((Traversal.Admin) traversal))
                            .addConfig(PlanConfig.QUERY_CONFIG, PlanUtils.getDefaultConfig(queryId));
                    AbstractBuilder jobReqBuilder = new TraversalTranslator(traversalBuilder).translate();
                    FileUtils.writeStringToFile(new File("plan.log"), String.format("query-%d", queryId), StandardCharsets.UTF_8, true);
                    PlanUtils.print(jobReqBuilder);
                    broadcastProcessor.broadcast(jobReqBuilder.build(), ctx, new GremlinResultProcessor(ctx, new DefaultResultParser(traversalBuilder)));
                    logger.info("query-{} finish", queryId);
                });
                return op;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close() throws Exception {
        this.broadcastProcessor.close();
    }
}
