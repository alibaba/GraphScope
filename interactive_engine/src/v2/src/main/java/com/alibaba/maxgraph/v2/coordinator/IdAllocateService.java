package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.AllocateIdRequest;
import com.alibaba.maxgraph.proto.v2.AllocateIdResponse;
import com.alibaba.maxgraph.proto.v2.IdAllocateGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdAllocateService extends IdAllocateGrpc.IdAllocateImplBase {

    private static final Logger logger = LoggerFactory.getLogger(IdAllocateService.class);

    private IdAllocator idAllocator;

    public IdAllocateService(IdAllocator idAllocator) {
        this.idAllocator = idAllocator;
    }

    @Override
    public void allocateId(AllocateIdRequest request, StreamObserver<AllocateIdResponse> responseObserver) {
        int allocateSize = request.getAllocateSize();
        try {
            long startId = this.idAllocator.allocate(allocateSize);
            responseObserver.onNext(AllocateIdResponse.newBuilder().setStartId(startId).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("allocate id failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
