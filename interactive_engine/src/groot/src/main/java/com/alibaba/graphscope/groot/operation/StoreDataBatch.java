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

import com.alibaba.maxgraph.proto.groot.PartitionToBatchPb;
import com.alibaba.maxgraph.proto.groot.StoreDataBatchPb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreDataBatch {
    private String requestId;
    private int queueId;
    private long snapshotId;
    private long offset;
    // List [ partition -> OperationBatch ]
    private List<Map<Integer, OperationBatch>> dataBatch;

    private StoreDataBatch(
            String requestId,
            int queueId,
            long snapshotId,
            long offset,
            List<Map<Integer, OperationBatch>> dataBatch) {
        this.requestId = requestId;
        this.queueId = queueId;
        this.snapshotId = snapshotId;
        this.offset = offset;
        this.dataBatch = Collections.unmodifiableList(new ArrayList<>(dataBatch));
    }

    public static StoreDataBatch parseProto(StoreDataBatchPb proto) {
        String requestId = proto.getRequestId();
        int queueId = proto.getQueueId();
        long snapshotId = proto.getSnapshotId();
        long offset = proto.getOffset();
        List<PartitionToBatchPb> batchPbList = proto.getDataBatchList();
        List<Map<Integer, OperationBatch>> dataBatch = new ArrayList<>(batchPbList.size());
        for (PartitionToBatchPb batchPb : batchPbList) {
            Map<Integer, OperationBatch> batch = new HashMap<>();
            batchPb.getPartitionToBatchMap()
                    .forEach((pid, pb) -> batch.put(pid, OperationBatch.parseProto(pb)));
            dataBatch.add(batch);
        }
        return new StoreDataBatch(requestId, queueId, snapshotId, offset, dataBatch);
    }

    public String getRequestId() {
        return requestId;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getOffset() {
        return offset;
    }

    public List<Map<Integer, OperationBatch>> getDataBatch() {
        return dataBatch;
    }

    public StoreDataBatchPb toProto() {
        StoreDataBatchPb.Builder builder = StoreDataBatchPb.newBuilder();
        builder.setRequestId(requestId)
                .setQueueId(queueId)
                .setSnapshotId(snapshotId)
                .setOffset(offset);
        for (Map<Integer, OperationBatch> batch : dataBatch) {
            PartitionToBatchPb.Builder batchBuilder = PartitionToBatchPb.newBuilder();
            batch.forEach((pid, ops) -> batchBuilder.putPartitionToBatch(pid, ops.toProto()));
            builder.addDataBatch(batchBuilder);
        }
        return builder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String requestId;
        private int queueId;
        private long snapshotId;
        private long offset;
        private List<Map<Integer, OperationBatch>> dataBatch;
        private Map<Integer, OperationBatch.Builder> partitionBatchBuilder;

        public Builder() {
            this.dataBatch = new ArrayList<>();
            this.partitionBatchBuilder = new HashMap<>();
        }

        public Builder requestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder queueId(int queueId) {
            this.queueId = queueId;
            return this;
        }

        public Builder snapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder addBatch(Map<Integer, OperationBatch> batch) {
            this.dataBatch.add(batch);
            return this;
        }

        public Builder addOperation(int partitionId, OperationBlob operationBlob) {
            if (partitionBatchBuilder.size() > 0) {
                if (partitionId == -1 ^ partitionBatchBuilder.containsKey(-1)) {
                    // Build a batch
                    Map<Integer, OperationBatch> batch =
                            new HashMap<>(partitionBatchBuilder.size());
                    partitionBatchBuilder.forEach(
                            (pid, builder) -> batch.put(pid, builder.build()));
                    addBatch(batch);
                    partitionBatchBuilder = new HashMap<>();
                }
            }
            OperationBatch.Builder builder =
                    partitionBatchBuilder.computeIfAbsent(
                            partitionId, k -> OperationBatch.newBuilder());
            builder.addOperationBlob(operationBlob);
            return this;
        }

        public StoreDataBatch build() {
            if (partitionBatchBuilder.size() > 0) {
                Map<Integer, OperationBatch> batch = new HashMap<>(partitionBatchBuilder.size());
                partitionBatchBuilder.forEach((pid, builder) -> batch.put(pid, builder.build()));
                addBatch(batch);
            }
            return new StoreDataBatch(requestId, queueId, snapshotId, offset, dataBatch);
        }
    }
}
