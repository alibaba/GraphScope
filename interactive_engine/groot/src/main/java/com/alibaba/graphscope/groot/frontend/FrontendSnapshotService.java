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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.groot.FrontendSnapshotGrpc;
import com.alibaba.graphscope.groot.schema.GraphDef;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontendSnapshotService extends FrontendSnapshotGrpc.FrontendSnapshotImplBase {

    private static final Logger logger = LoggerFactory.getLogger(FrontendSnapshotService.class);

    private SnapshotCache snapshotCache;

    public FrontendSnapshotService(SnapshotCache snapshotCache) {
        this.snapshotCache = snapshotCache;
    }

    @Override
    public void advanceQuerySnapshot(
            AdvanceQuerySnapshotRequest request,
            StreamObserver<AdvanceQuerySnapshotResponse> observer) {
        try {
            long snapshotId = request.getSnapshotId();
            GraphDef graphDef = GraphDef.parseProto(request.getGraphDef());
            long prevSnapshotId = snapshotCache.advanceQuerySnapshotId(snapshotId, graphDef);
            AdvanceQuerySnapshotResponse response =
                    AdvanceQuerySnapshotResponse.newBuilder()
                            .setPreviousSnapshotId(prevSnapshotId)
                            .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            logger.error("error advance query snapshot", e);
            observer.onError(e);
        }
    }
}
