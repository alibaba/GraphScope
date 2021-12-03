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

import com.alibaba.maxgraph.proto.groot.OperationPb;
import com.google.protobuf.ByteString;

public class OperationBlob {
    public static final OperationBlob MARKER_OPERATION_BLOB = new MarkerOperation().toBlob();

    private long partitionKey;
    private OperationType operationType;
    private ByteString dataBytes;

    public OperationBlob(long partitionKey, OperationType operationType, ByteString dataBytes) {
        this.partitionKey = partitionKey;
        this.operationType = operationType;
        this.dataBytes = dataBytes;
    }

    public static OperationBlob parseProto(OperationPb proto) {
        long partitionKey = proto.getPartitionKey();
        OperationType operationType = OperationType.parseProto(proto.getOpType());
        ByteString bytes = proto.getDataBytes();
        return new OperationBlob(partitionKey, operationType, bytes);
    }

    public long getPartitionKey() {
        return partitionKey;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public OperationPb toProto() {
        return OperationPb.newBuilder()
                .setPartitionKey(partitionKey)
                .setOpType(operationType.toProto())
                .setDataBytes(dataBytes)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationBlob that = (OperationBlob) o;

        if (partitionKey != that.partitionKey) return false;
        if (operationType != that.operationType) return false;
        return dataBytes != null ? dataBytes.equals(that.dataBytes) : that.dataBytes == null;
    }
}
