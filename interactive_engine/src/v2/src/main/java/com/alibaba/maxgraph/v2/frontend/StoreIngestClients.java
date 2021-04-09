package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.function.Function;

public class StoreIngestClients extends RoleClients<StoreIngestClient> implements StoreIngestor {

    public StoreIngestClients(ChannelManager channelManager, RoleType targetRole,
                              Function<ManagedChannel, StoreIngestClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }


    @Override
    public void ingest(int storeId, String path, CompletionCallback<Void> callback) {
        this.getClient(storeId).storeIngest(path, callback);
    }
}
