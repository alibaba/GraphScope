package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;

public interface StoreWriter {
    void write(int storeId, StoreDataBatch storeDataBatch, CompletionCallback<Integer> callback);
}
