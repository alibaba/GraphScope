package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.function.Function;

public class IngestProgressClients extends RoleClients<IngestProgressClient> implements IngestProgressFetcher {

    public IngestProgressClients(ChannelManager channelManager, RoleType targetRole,
                                 Function<ManagedChannel, IngestProgressClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }

    @Override
    public List<Long> getTailOffsets(List<Integer> queueIds) {
        return getClient(0).getTailOffsets(queueIds);
    }
}
