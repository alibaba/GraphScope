package com.alibaba.maxgraph.v2.frontend.server.loader;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.loader.MaxGraphOpLoader;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.processor.MaxGraphProcessor;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.processor.MaxGraphTraversalProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

/**
 * Load maxgraph processor instead of tinkerpop processor
 */
public class MaxGraphProcessorLoader implements ProcessorLoader {
    private Configs configs;
    private SchemaFetcher schemaFetcher;
    private GraphPartitionManager partitionManager;
    private RoleClients<QueryExecuteRpcClient> queryExecuteClients;
    private RoleClients<QueryStoreRpcClient> queryStoreClients;
    private RoleClients<QueryManageRpcClient> queryManageClients;
    private int executorCount;
    private GraphWriterContext graphWriterContext;

    public MaxGraphProcessorLoader(Configs configs,
                                   SchemaFetcher schemaFetcher,
                                   GraphPartitionManager partitionManager,
                                   RoleClients<QueryExecuteRpcClient> queryExecuteClients,
                                   RoleClients<QueryStoreRpcClient> queryStoreClients,
                                   RoleClients<QueryManageRpcClient> queryManageClients,
                                   int executorCount,
                                   GraphWriterContext graphWriterContext) {
        this.configs = configs;
        this.schemaFetcher = schemaFetcher;
        this.partitionManager = partitionManager;
        this.queryExecuteClients = queryExecuteClients;
        this.queryStoreClients = queryStoreClients;
        this.queryManageClients = queryManageClients;
        this.executorCount = executorCount;
        this.graphWriterContext = graphWriterContext;
    }

    @Override
    public void loadProcessor(Settings settings) {
        OpLoader.init(settings);

        MaxGraphProcessor maxGraphProcessor = new MaxGraphProcessor(configs,
                schemaFetcher,
                this.partitionManager,
                this.queryStoreClients,
                this.queryExecuteClients,
                this.queryManageClients,
                this.executorCount,
                this.graphWriterContext);
        maxGraphProcessor.init(settings);
        // replace StandardOpProcessor
        MaxGraphOpLoader.addOpProcessor(maxGraphProcessor.getName(), maxGraphProcessor);

        MaxGraphTraversalProcessor traversalProcessor = new MaxGraphTraversalProcessor(configs,
                schemaFetcher,
                this.partitionManager,
                this.queryStoreClients,
                this.queryExecuteClients,
                this.queryManageClients,
                this.executorCount,
                this.graphWriterContext);
        traversalProcessor.init(settings);
        // replace traversal processor
        MaxGraphOpLoader.addOpProcessor(traversalProcessor.getName(), traversalProcessor);
    }
}
