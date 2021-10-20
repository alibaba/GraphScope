/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.operation;

import com.alibaba.maxgraph.proto.groot.OperationBatchPb;
import com.alibaba.maxgraph.proto.groot.OperationPb;

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

        return operationBlobs != null
                ? operationBlobs.equals(that.operationBlobs)
                : that.operationBlobs == null;
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
