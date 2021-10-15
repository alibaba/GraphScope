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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.proto.groot.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.CommitSnapshotIdResponse;
import com.alibaba.maxgraph.proto.groot.SnapshotCommitGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class SnapshotCommitService extends SnapshotCommitGrpc.SnapshotCommitImplBase {

    private SnapshotManager snapshotManager;

    public SnapshotCommitService(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void commitSnapshotId(
            CommitSnapshotIdRequest request,
            StreamObserver<CommitSnapshotIdResponse> responseObserver) {
        int storeId = request.getStoreId();
        long snapshotId = request.getSnapshotId();
        long ddlSnapshotId = request.getDdlSnapshotId();
        List<Long> queueOffsets = request.getQueueOffsetsList();
        // prevent gRPC auto-cancellation
        Context.current()
                .fork()
                .run(
                        () ->
                                this.snapshotManager.commitSnapshotId(
                                        storeId, snapshotId, ddlSnapshotId, queueOffsets));
        responseObserver.onNext(CommitSnapshotIdResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
