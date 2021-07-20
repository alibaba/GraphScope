package com.alibaba.maxgraph.v2.frontend.write;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;

import java.util.concurrent.atomic.AtomicLong;

public class DefaultEdgeIdGenerator extends RoleClients<IdAllocateClient> implements EdgeIdGenerator {

    private int idAllocateSize;
    private AtomicLong currentId = new AtomicLong(0);
    private volatile long upper = 0L;

    public DefaultEdgeIdGenerator(Configs configs, ChannelManager channelManager) {
        super(channelManager, RoleType.COORDINATOR, IdAllocateClient::new);
        this.idAllocateSize = CommonConfig.ID_ALLOCATE_SIZE.get(configs);
    }

    @Override
    public long getNextId() {
        long newId = currentId.getAndIncrement();
        long currentUpper = this.upper;
        if (newId < currentUpper) {
            return newId;
        }
        synchronized (this) {
            if (currentUpper == this.upper) {
                allocateNewIds();
            }
        }
        return getNextId();
    }

    private void allocateNewIds() {
        long startId = getClient(0).allocateId(this.idAllocateSize);
        this.currentId.set(startId);
        this.upper = startId + this.idAllocateSize;
    }
}
