package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.frontend.write.KafkaAppender;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.graphscope.proto.groot.IngestorSnapshotGrpc;

import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestorSnapshotService extends IngestorSnapshotGrpc.IngestorSnapshotImplBase {
    private static final Logger logger = LoggerFactory.getLogger(IngestorSnapshotService.class);

    private final KafkaAppender kafkaAppender;

    public IngestorSnapshotService(KafkaAppender kafkaAppender) {
        this.kafkaAppender = kafkaAppender;
    }

    @Override
    public void advanceIngestSnapshotId(
            AdvanceIngestSnapshotIdRequest request,
            StreamObserver<AdvanceIngestSnapshotIdResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        this.kafkaAppender.advanceIngestSnapshotId(
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
