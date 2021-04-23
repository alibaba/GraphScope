package com.alibaba.maxgraph.v2.frontend.server.session;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.StoreConfig;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphReader;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
import com.alibaba.maxgraph.v2.common.frontend.remote.RemoteMaxGraphReader;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.MaxGraphWriterImpl;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.config.GraphStoreType;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphReader;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMaxGraphWriter;
import com.alibaba.maxgraph.v2.frontend.graph.memory.DefaultMemoryGraph;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;

/**
 * Graph session factory, it will create writer/reader for each query with given store type
 */
public final class GraphSessionFactory {
    private GraphStoreType graphStoreType;

    public GraphSessionFactory(GraphStoreType graphStoreType) {
        this.graphStoreType = graphStoreType;
    }

    public MaxGraphWriter buildGraphWriter(SessionManager sessionManager,
                                           SnapshotSchema snapshotSchema,
                                           GraphWriterContext graphWriterContext,
                                           MaxGraphCache cache) {
        switch (graphStoreType) {
            case MAXGRAPH:
                return new MaxGraphWriterImpl(graphWriterContext.getRealtimeWriter(),
                        graphWriterContext.getSchemaWriter(),
                        graphWriterContext.getDdlExecutors(),
                        graphWriterContext.getSchemaCache(),
                        sessionManager.getSessionId(),
                        graphWriterContext.isAutoCommit(),
                        cache);
            case MEMORY:
                return new DefaultMaxGraphWriter(
                        sessionManager.getRequestId(),
                        sessionManager.getSessionId(),
                        (DefaultGraphSchema) snapshotSchema.getSchema(),
                        DefaultMemoryGraph.getGraph());
            default:
                throw new IllegalArgumentException("Invalid graph type " + graphStoreType.toString());
        }
    }

    public MaxGraphReader buildGraphReader(SnapshotMaxGraph graph,
                                           MaxGraphWriter writer,
                                           GraphPartitionManager partitionManager,
                                           RoleClients<QueryStoreRpcClient> queryStoreClient,
                                           SchemaFetcher schemaFetcher,
                                           Configs configs,
                                           MaxGraphCache cache) {
        switch (graphStoreType) {
            case MAXGRAPH: {
                return new RemoteMaxGraphReader(partitionManager, queryStoreClient, schemaFetcher, graph, CommonConfig.STORE_NODE_COUNT.get(configs), cache);
            }
            case MEMORY: {
                return new DefaultMaxGraphReader(writer,
                        DefaultMemoryGraph.getGraph(),
                        schemaFetcher);
            }
            default: {
                throw new IllegalArgumentException("Invalid graph type " + graphStoreType.toString());
            }
        }
    }
}
