package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.function.Function;

public class SnapshotCommitClients extends RoleClients<SnapshotCommitClient> implements SnapshotCommitter {

    public SnapshotCommitClients(ChannelManager channelManager, RoleType targetRole,
                                 Function<ManagedChannel, SnapshotCommitClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }

    @Override
    public void commitSnapshotId(int storeId, long snapshotId, long ddlSnapshotId, List<Long> offsets) {
        getClient(0).commitSnapshotId(storeId, snapshotId, ddlSnapshotId, offsets);
    }
}
