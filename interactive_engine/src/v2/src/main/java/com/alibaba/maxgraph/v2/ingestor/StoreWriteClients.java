package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.function.Function;

public class StoreWriteClients extends RoleClients<StoreWriteClient> implements StoreWriter {

    public StoreWriteClients(ChannelManager channelManager, RoleType targetRole,
                             Function<ManagedChannel, StoreWriteClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }

    @Override
    public void write(int storeId, StoreDataBatch storeDataBatch, CompletionCallback<Integer> callback) {
        this.getClient(storeId).writeStore(storeDataBatch, callback);
    }
}
