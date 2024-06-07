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
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.rpc.RpcChannel;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.CommitSnapshotIdRequest;
import com.alibaba.graphscope.proto.groot.SnapshotCommitGrpc;

import io.grpc.ManagedChannel;

import java.util.List;

public class SnapshotCommitClient extends RpcClient {
    public SnapshotCommitClient(RpcChannel channel) {
        super(channel);
    }

    public SnapshotCommitClient(SnapshotCommitGrpc.SnapshotCommitBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
    }

    private SnapshotCommitGrpc.SnapshotCommitBlockingStub getStub() {
        return SnapshotCommitGrpc.newBlockingStub(rpcChannel.getChannel());
    }

    public void commitSnapshotId(
            int storeId, long snapshotId, long ddlSnapshotId, List<Long> queueOffsets) {
        CommitSnapshotIdRequest req =
                CommitSnapshotIdRequest.newBuilder()
                        .setStoreId(storeId)
                        .setSnapshotId(snapshotId)
                        .setDdlSnapshotId(ddlSnapshotId)
                        .addAllQueueOffsets(queueOffsets)
                        .build();
        getStub().commitSnapshotId(req);
    }
}
