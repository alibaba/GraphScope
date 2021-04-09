package com.alibaba.maxgraph.v2.ingestor;

public interface IngestCallback {
    void onSuccess(long snapshotId);

    void onFailure(Exception e);
}
