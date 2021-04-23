package com.alibaba.maxgraph.v2.frontend.graph.transaction;

import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

import java.util.concurrent.TimeUnit;

public class SnapshotBatchTransaction extends AbstractThreadLocalTransaction {
    private static final long COMMIT_TIMEOUT_SEC = 30;
    private SnapshotMaxGraph graph;
    private boolean autoCommitEnable = true;

    public SnapshotBatchTransaction(SnapshotMaxGraph graph) {
        super(graph);
        this.graph = graph;
    }


    @Override
    protected void doOpen() {
        this.autoCommitEnable = false;
        graph.getGraphWriter().setAutoCommit(this.autoCommitEnable);

    }

    @Override
    protected void doCommit() throws TransactionException {
        if (!this.autoCommitEnable) {
            try {
                graph.getGraphWriter().commit().get(COMMIT_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new TransactionException("do commit failed", e);
            }
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        throw new UnsupportedOperationException("Not support rollback");
    }

    @Override
    public boolean isOpen() {
        return !this.autoCommitEnable;
    }
}
