package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.proto.v2.OperationBatchPb;
import com.alibaba.maxgraph.proto.v2.OperationPb;
import com.alibaba.maxgraph.v2.common.operation.Operation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OperationBatch implements Iterable<OperationBlob> {

    private long latestSnapshotId;
    private List<OperationBlob> operationBlobs;

    private OperationBatch(long latestSnapshotId, List<OperationBlob> operationBlobs) {
        this.latestSnapshotId = latestSnapshotId;
        this.operationBlobs = operationBlobs;
    }

    public static OperationBatch parseProto(OperationBatchPb proto) {
        long latestSnapshotId = proto.getLatestSnapshotId();
        List<OperationPb> operationPbs = proto.getOperationsList();
        List<OperationBlob> operationBlobs = new ArrayList<>(operationPbs.size());
        for (OperationPb operationPb : operationPbs) {
            operationBlobs.add(OperationBlob.parseProto(operationPb));
        }
        return new OperationBatch(latestSnapshotId, operationBlobs);
    }

    public int getOperationCount() {
        return operationBlobs.size();
    }

    @Override
    public Iterator<OperationBlob> iterator() {
        return operationBlobs.iterator();
    }

    public long getLatestSnapshotId() {
        return latestSnapshotId;
    }

    public OperationBlob getOperationBlob(int i) {
        return operationBlobs.get(i);
    }

    public OperationBatchPb toProto() {
        OperationBatchPb.Builder builder = OperationBatchPb.newBuilder();
        builder.setLatestSnapshotId(latestSnapshotId);
        for (OperationBlob operationBlob : operationBlobs) {
            builder.addOperations(operationBlob.toProto());
        }
        return builder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(List<Operation> operations) {
        return new Builder(operations);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationBatch that = (OperationBatch) o;

        return operationBlobs != null ? operationBlobs.equals(that.operationBlobs) : that.operationBlobs == null;
    }

    public static class Builder {

        private boolean built = false;
        private long latestSnapshotId;
        private List<OperationBlob> operationBlobs;

        private Builder() {
            this.latestSnapshotId = 0L;
            this.operationBlobs = new ArrayList<>();
        }

        private Builder(List<Operation> operations) {
            this();
            for (Operation operation : operations) {
                addOperation(operation);
            }
        }

        public Builder addOperation(Operation operation) {
            return addOperationBlob(operation.toBlob());
        }

        public Builder addOperationBlob(OperationBlob operationBlob) {
            if (this.built) {
                throw new IllegalStateException("cannot add operation after built");
            }
            this.operationBlobs.add(operationBlob);
            return this;
        }

        public Builder setLatestSnapshotId(long latestSnapshotId) {
            this.latestSnapshotId = latestSnapshotId;
            return this;
        }

        public OperationBatch build() {
            this.built = true;
            return new OperationBatch(latestSnapshotId, operationBlobs);
        }
    }
}
