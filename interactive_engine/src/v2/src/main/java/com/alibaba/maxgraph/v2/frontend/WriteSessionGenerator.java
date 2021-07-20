package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.frontend.utils.WriteSessionUtil;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class WriteSessionGenerator {

    private AtomicLong nextIdx;
    private int frontendNodeId;

    public WriteSessionGenerator(Configs configs) {
        int frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
        int startIdx = new Random().nextInt(frontendCount);
        this.nextIdx = new AtomicLong(startIdx);
        this.frontendNodeId = CommonConfig.NODE_IDX.get(configs);
    }

    public String newWriteSession() {
        long nextClientIdx = this.nextIdx.getAndIncrement();
        return WriteSessionUtil.createWriteSession(frontendNodeId, nextClientIdx, System.currentTimeMillis());
    }
}
