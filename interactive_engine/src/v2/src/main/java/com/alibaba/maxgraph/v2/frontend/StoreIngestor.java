package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.CompletionCallback;

public interface StoreIngestor {
    void ingest(int storeId, String path, CompletionCallback<Void> callback);
}
