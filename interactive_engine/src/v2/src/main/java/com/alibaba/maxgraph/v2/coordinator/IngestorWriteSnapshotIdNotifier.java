package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestorWriteSnapshotIdNotifier implements WriteSnapshotIdNotifier {
    private static final Logger logger = LoggerFactory.getLogger(IngestorWriteSnapshotIdNotifier.class);

    private RoleClients<IngestorSnapshotClient> ingestorSnapshotClients;
    private int ingestorCount;

    public IngestorWriteSnapshotIdNotifier(Configs configs, RoleClients<IngestorSnapshotClient> ingestorSnapshotClients) {
        this.ingestorSnapshotClients = ingestorSnapshotClients;
        this.ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(configs);
    }

    @Override
    public void notifyWriteSnapshotIdChanged(long snapshotId) {
        for (int i = 0; i < this.ingestorCount; i++) {
            try {
                int realtimeWriterId = i;
                this.ingestorSnapshotClients.getClient(realtimeWriterId).advanceIngestSnapshotId(snapshotId,
                        new CompletionCallback<Long>() {
                            @Override
                            public void onCompleted(Long previousSnapshotId) {
                                if (previousSnapshotId > snapshotId) {
                                    logger.error("unexpected previousSnapshotId [" + previousSnapshotId + "], " +
                                            "should <= [" + snapshotId + "]. " +
                                            "target realtimeWriter [" + realtimeWriterId + "]");
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                logger.error("error in advanceIngestSnapshotId [" + snapshotId +
                                                "]. realtimeWriter [" + realtimeWriterId + "]", t);
                            }
                        });
            } catch (Exception e) {
                logger.warn("update writeSnapshotId failed. realtimeWriter [" + i + "]", e);
            }
        }
    }
}
