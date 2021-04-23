package com.alibaba.maxgraph.v2.common;

import java.util.List;

public interface MetaService {
    void start();

    void stop();

    int getPartitionCount();

    int getStoreIdByPartition(int partitionId);

    List<Integer> getPartitionsByStoreId(int storeId);

    int getQueueCount();

    List<Integer> getQueueIdsForIngestor(int ingestorId);

    int getIngestorIdForQueue(int queueId);

    int getStoreCount();
}
