package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.frontend.write.KafkaAppender;
import com.alibaba.graphscope.groot.ingestor.IngestCallback;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.proto.groot.IngestorWriteGrpc;
import com.alibaba.graphscope.proto.groot.WriteIngestorRequest;
import com.alibaba.graphscope.proto.groot.WriteIngestorResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class IngestorWriteService extends IngestorWriteGrpc.IngestorWriteImplBase {
    private static final Logger logger = LoggerFactory.getLogger(IngestorWriteService.class);

    private final KafkaAppender appender;
    public IngestorWriteService(KafkaAppender appender) {
        this.appender = appender;
    }

    @Override
    public void writeIngestor(
            WriteIngestorRequest request, StreamObserver<WriteIngestorResponse> responseObserver) {
        try {
            int queueId = request.getQueueId();
            String requestId = request.getRequestId();
            OperationBatch operationBatch = OperationBatch.parseProto(request.getOperationBatch());
            this.appender.ingestBatch(
                    requestId,
                    operationBatch,
                    new IngestCallback() {
                        @Override
                        public void onSuccess(long snapshotId) {
                            WriteIngestorResponse response =
                                    WriteIngestorResponse.newBuilder()
                                            .setSnapshotId(snapshotId)
                                            .build();
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            responseObserver.onError(e);
                        }
                    });
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
