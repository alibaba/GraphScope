package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.exception.UnsupportedOperationException;
import com.alibaba.graphscope.groot.common.util.PkHashUtils;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultEdgeIdGenerator extends RoleClients<IdAllocateClient>
        implements EdgeIdGenerator {

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

    @Override
    public long getHashId(long srcId, long dstId, int labelId, List<byte[]> pks) {
        if (pks == null || pks.size() == 0) {
            throw new UnsupportedOperationException("Cannot get hash id when pk is empty");
        }
        return PkHashUtils.hash(srcId, dstId, labelId, pks);
    }

    private void allocateNewIds() {
        long startId = getClient(0).allocateId(this.idAllocateSize);
        this.currentId.set(startId);
        this.upper = startId + this.idAllocateSize;
    }
}
