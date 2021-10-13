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

import com.alibaba.maxgraph.proto.groot.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.groot.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.groot.IngestProgressGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IngestProgressService extends IngestProgressGrpc.IngestProgressImplBase {
    private static final Logger logger = LoggerFactory.getLogger(IngestProgressService.class);

    private SnapshotManager snapshotManager;

    public IngestProgressService(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void getTailOffsets(
            GetTailOffsetsRequest request,
            StreamObserver<GetTailOffsetsResponse> responseObserver) {
        logger.info("Get offset of [" + request.getQueueIdList() + "]");
        List<Integer> queueIdList = request.getQueueIdList();
        List<Long> tailOffsets = this.snapshotManager.getTailOffsets(queueIdList);
        GetTailOffsetsResponse response =
                GetTailOffsetsResponse.newBuilder().addAllOffsets(tailOffsets).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
