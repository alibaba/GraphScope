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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FrontendStoreService extends FrontendStoreServiceGrpc.FrontendStoreServiceImplBase {

    private final StoreService storeService;
    private final KafkaProcessor processor;

    public FrontendStoreService(StoreService storeService, KafkaProcessor processor) {
        this.storeService = storeService;
        this.processor = processor;
    }

    @Override
    public void storeIngest(
            IngestDataRequest request, StreamObserver<IngestDataResponse> responseObserver) {
        String dataPath = request.getDataPath();
        Map<String, String> config = request.getConfigMap();
        this.storeService.ingestData(
                dataPath,
                config,
                new CompletionCallback<Void>() {
                    @Override
                    public void onCompleted(Void res) {
                        responseObserver.onNext(IngestDataResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(
                                Status.INTERNAL
                                        .withDescription(t.getMessage())
                                        .asRuntimeException());
                    }
                });
    }

    @Override
    public void storeClearIngest(
            ClearIngestRequest request, StreamObserver<ClearIngestResponse> responseObserver) {
        try {
            this.storeService.clearIngest(request.getDataPath());
            responseObserver.onNext(ClearIngestResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void compactDB(
            CompactDBRequest request, StreamObserver<CompactDBResponse> responseObserver) {
        this.storeService.compactDB(
                new CompletionCallback<Void>() {
                    @Override
                    public void onCompleted(Void res) {
                        responseObserver.onNext(CompactDBResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(
                                Status.INTERNAL
                                        .withDescription(t.getMessage())
                                        .asRuntimeException());
                    }
                });
    }

    @Override
    public void reopenSecondary(
            ReopenSecondaryRequest request,
            StreamObserver<ReopenSecondaryResponse> responseObserver) {
        this.storeService.reopenPartition(
                5,
                new CompletionCallback<Void>() {
                    @Override
                    public void onCompleted(Void res) {
                        responseObserver.onNext(ReopenSecondaryResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(
                                Status.INTERNAL
                                        .withDescription(t.getMessage())
                                        .asRuntimeException());
                    }
                });
    }

    @Override
    public void getState(
            GetStoreStateRequest request, StreamObserver<GetStoreStateResponse> responseObserver) {
        long[] spaces = this.storeService.getDiskStatus();
        GetStoreStateResponse.Builder builder = GetStoreStateResponse.newBuilder();
        PartitionStatePb state =
                PartitionStatePb.newBuilder()
                        .setTotalSpace(spaces[0])
                        .setUsableSpace(spaces[1])
                        .build();

        builder.putPartitionStates(storeService.getStoreId(), state);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void replayRecordsV2(
            ReplayRecordsRequestV2 request,
            StreamObserver<ReplayRecordsResponseV2> responseObserver) {
        try {
            long offset = request.getOffset();
            long ts = request.getTimestamp();
            List<Long> si = this.processor.replayDMLRecordsFrom(offset, ts);
            responseObserver.onNext(
                    ReplayRecordsResponseV2.newBuilder().addAllSnapshotId(si).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
