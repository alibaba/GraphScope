package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.proto.write.*;
import com.alibaba.maxgraph.common.util.UuidUtils;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.frontend.write.WriteRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ClientWriteService extends ClientWriteGrpc.ClientWriteImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ClientWriteService.class);

    private WriteSessionGenerator writeSessionGenerator;
    private GraphWriter graphWriter;

    public ClientWriteService(
            WriteSessionGenerator writeSessionGenerator, GraphWriter graphWriter) {
        this.writeSessionGenerator = writeSessionGenerator;
        this.graphWriter = graphWriter;
    }

    @Override
    public void getClientId(
            GetClientIdRequest request, StreamObserver<GetClientIdResponse> responseObserver) {
        String writeSession = writeSessionGenerator.newWriteSession();
        responseObserver.onNext(GetClientIdResponse.newBuilder().setClientId(writeSession).build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchWrite(
            BatchWriteRequest request, StreamObserver<BatchWriteResponse> responseObserver) {
        String requestId = UuidUtils.getBase64UUIDString();
        String writeSession = request.getClientId();
        int writeRequestsCount = request.getWriteRequestsCount();
        List<WriteRequest> writeRequests = new ArrayList<>(writeRequestsCount);
        logger.info(
                "received batchWrite request. requestId ["
                        + requestId
                        + "] writeSession ["
                        + writeSession
                        + "] batchSize ["
                        + writeRequestsCount
                        + "]");
        try {
            for (WriteRequestPb writeRequestPb : request.getWriteRequestsList()) {
                writeRequests.add(WriteRequest.parseProto(writeRequestPb));
            }
            long snapshotId = graphWriter.writeBatch(requestId, writeSession, writeRequests);
            responseObserver.onNext(
                    BatchWriteResponse.newBuilder().setSnapshotId(snapshotId).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error(
                    "batchWrite failed. request [" + requestId + "] session [" + writeSession + "]",
                    e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void remoteFlush(
            RemoteFlushRequest request, StreamObserver<RemoteFlushResponse> responseObserver) {
        long flushSnapshotId = request.getSnapshotId();
        long waitTimeMs = request.getWaitTimeMs();
        logger.info(
                "flush snapshot id [" + flushSnapshotId + "] with timeout [" + waitTimeMs + "]ms");
        try {
            boolean suc;
            if (flushSnapshotId == 0L) {
                suc = graphWriter.flushLastSnapshot(waitTimeMs);
            } else {
                suc = graphWriter.flushSnapshot(flushSnapshotId, waitTimeMs);
            }
            responseObserver.onNext(RemoteFlushResponse.newBuilder().setSuccess(suc).build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            logger.error(
                    "remoteFlush failed. flushSnapshotId ["
                            + flushSnapshotId
                            + "] waitTimeMs ["
                            + waitTimeMs
                            + "]");
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
