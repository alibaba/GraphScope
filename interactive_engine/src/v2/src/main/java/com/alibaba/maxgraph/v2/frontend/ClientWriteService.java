package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.graphscope.proto.write.*;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import com.alibaba.maxgraph.v2.frontend.write.GraphWriter;
import com.alibaba.maxgraph.v2.frontend.write.WriteRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class ClientWriteService extends ClientWriteGrpc.ClientWriteImplBase {

    private WriteSessionGenerator writeSessionGenerator;
    private GraphWriter graphWriter;

    public ClientWriteService(WriteSessionGenerator writeSessionGenerator, GraphWriter graphWriter) {
        this.writeSessionGenerator = writeSessionGenerator;
        this.graphWriter = graphWriter;
    }

    @Override
    public void getClientId(GetClientIdRequest request, StreamObserver<GetClientIdResponse> responseObserver) {
        String writeSession = writeSessionGenerator.newWriteSession();
        responseObserver.onNext(GetClientIdResponse.newBuilder().setClientId(writeSession).build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchWrite(BatchWriteRequest request, StreamObserver<BatchWriteResponse> responseObserver) {
        String requestId = UuidUtils.getBase64UUIDString();
        String writeSession = request.getClientId();
        List<WriteRequest> writeRequests = new ArrayList<>(request.getWriteRequestsCount());
        try {
            for (WriteRequestPb writeRequestPb : request.getWriteRequestsList()) {
                writeRequests.add(WriteRequest.parseProto(writeRequestPb));
            }
            long snapshotId = graphWriter.writeBatch(requestId, writeSession, writeRequests);
            responseObserver.onNext(BatchWriteResponse.newBuilder().setSnapshotId(snapshotId).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void remoteFlush(RemoteFlushRequest request, StreamObserver<RemoteFlushResponse> responseObserver) {
        long flushSnapshotId = request.getSnapshotId();
        long waitTimeMs = request.getWaitTimeMs();
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
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
