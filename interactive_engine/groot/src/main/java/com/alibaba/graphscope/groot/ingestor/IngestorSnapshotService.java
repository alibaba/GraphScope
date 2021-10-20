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
package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.maxgraph.proto.groot.IngestorSnapshotGrpc;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import io.grpc.stub.StreamObserver;

public class IngestorSnapshotService extends IngestorSnapshotGrpc.IngestorSnapshotImplBase {

    private IngestService ingestService;

    public IngestorSnapshotService(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @Override
    public void advanceIngestSnapshotId(
            AdvanceIngestSnapshotIdRequest request,
            StreamObserver<AdvanceIngestSnapshotIdResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        this.ingestService.advanceIngestSnapshotId(
                snapshotId,
                new CompletionCallback<Long>() {
                    @Override
                    public void onCompleted(Long previousSnapshotId) {
                        AdvanceIngestSnapshotIdResponse response =
                                AdvanceIngestSnapshotIdResponse.newBuilder()
                                        .setPreviousSnapshotId(previousSnapshotId)
                                        .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }
                });
    }
}
