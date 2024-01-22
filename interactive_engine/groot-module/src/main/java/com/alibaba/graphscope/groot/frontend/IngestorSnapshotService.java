package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.ingestor.IngestService;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.graphscope.proto.groot.IngestorSnapshotGrpc;
import io.grpc.stub.StreamObserver;

public class IngestorSnapshotService extends IngestorSnapshotGrpc.IngestorSnapshotImplBase {

    private final GraphWriter graphWriter;
    public IngestorSnapshotService(GraphWriter graphWriter) {
        this.graphWriter = graphWriter;
    }

    @Override
    public void advanceIngestSnapshotId(
            AdvanceIngestSnapshotIdRequest request,
            StreamObserver<AdvanceIngestSnapshotIdResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        this.graphWriter.advanceIngestSnapshotId(
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
