package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class NotifyFrontendListener implements QuerySnapshotListener {
    private static final Logger logger = LoggerFactory.getLogger(NotifyFrontendListener.class);

    private int frontendId;
    private FrontendSnapshotClient frontendSnapshotClient;
    private SchemaManager schemaManager;

    private AtomicLong lastDdlSnapshotId;

    public NotifyFrontendListener(int frontendId, FrontendSnapshotClient frontendSnapshotClient,
                                  SchemaManager schemaManager) {
        this.frontendId = frontendId;
        this.frontendSnapshotClient = frontendSnapshotClient;
        this.schemaManager = schemaManager;

        this.lastDdlSnapshotId = new AtomicLong(-1L);
    }

    @Override
    public void snapshotAdvanced(long snapshotId, long ddlSnapshotId) {
        logger.debug("snapshot advance to [" + snapshotId + "]-[" + ddlSnapshotId + "], will notify frontend");
        GraphDef graphDef = null;
        if (ddlSnapshotId > this.lastDdlSnapshotId.get()) {
            graphDef = this.schemaManager.getGraphDef();
        }
        this.frontendSnapshotClient.advanceQuerySnapshot(snapshotId, graphDef, new CompletionCallback<Long>() {
            @Override
            public void onCompleted(Long res) {
                if (res >= snapshotId) {
                    logger.warn("unexpected previousSnapshotId [" + res + "], should <= [" + snapshotId +
                            "]. frontend [" + frontendId +"]");
                } else {
                    lastDdlSnapshotId.getAndUpdate(x -> x < ddlSnapshotId ? ddlSnapshotId : x);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("error in advanceQuerySnapshot [" + snapshotId + "], ddlSnapshotId [" + ddlSnapshotId +
                        "], frontend [" + frontendId + "]", t);
            }
        });
    }

}
