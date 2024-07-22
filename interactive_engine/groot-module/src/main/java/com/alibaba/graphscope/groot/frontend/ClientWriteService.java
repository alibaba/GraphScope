package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.util.Utils;
import com.alibaba.graphscope.groot.common.util.UuidUtils;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.frontend.write.WriteRequest;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientWriteService extends ClientWriteGrpc.ClientWriteImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ClientWriteService.class);
    private static Logger metricLogger = LoggerFactory.getLogger("MetricLog");

    private final GraphWriter graphWriter;

    public ClientWriteService(GraphWriter graphWriter) {
        this.graphWriter = graphWriter;
    }

    @Override
    public void getClientId(
            GetClientIdRequest request, StreamObserver<GetClientIdResponse> responseObserver) {
        responseObserver.onNext(
                GetClientIdResponse.newBuilder().setClientId("placeholder").build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchWrite(
            BatchWriteRequest request, StreamObserver<BatchWriteResponse> responseObserver) {
        long startBatchWrite = System.currentTimeMillis();
        String requestId = UuidUtils.getBase64UUIDString();
        String writeSession = request.getClientId();
        RequestOptionsPb optionsPb = request.getRequestOptions();
        String upTraceId = optionsPb == null ? null : optionsPb.getTraceId();
        int count = request.getWriteRequestsCount();
        List<WriteRequest> writeRequests = new ArrayList<>(count);
        logger.debug(
                "batchWrite: requestId {} writeSession {} batchSize {}",
                requestId,
                writeSession,
                count);
        try {
            for (WriteRequestPb writeRequestPb : request.getWriteRequestsList()) {
                writeRequests.add(WriteRequest.parseProto(writeRequestPb));
            }
            graphWriter.writeBatch(
                    requestId,
                    writeSession,
                    writeRequests,
                    optionsPb,
                    new CompletionCallback<Long>() {
                        @Override
                        public void onCompleted(Long res) {
                            long current = System.currentTimeMillis();
                            String metricJson =
                                    Utils.buildMetricJsonLog(
                                            true,
                                            upTraceId,
                                            count,
                                            null,
                                            (current - startBatchWrite),
                                            current,
                                            "writeKafka",
                                            "write");
                            metricLogger.info(metricJson);
                            responseObserver.onNext(
                                    BatchWriteResponse.newBuilder().setSnapshotId(res).build());
                            responseObserver.onCompleted();
                        }

                        @Override
                        public void onError(Throwable t) {
                            long current = System.currentTimeMillis();
                            String metricJson =
                                    Utils.buildMetricJsonLog(
                                            false,
                                            upTraceId,
                                            count,
                                            null,
                                            (current - startBatchWrite),
                                            current,
                                            "writeKafka",
                                            "write");
                            metricLogger.info(metricJson);
                            logger.error(
                                    "batch write error. request {} session {}",
                                    requestId,
                                    writeSession,
                                    t);
                            responseObserver.onError(
                                    Status.INTERNAL
                                            .withDescription(t.getMessage())
                                            .asRuntimeException());
                        }
                    });

        } catch (Exception e) {
            logger.error(
                    "batchWrite failed. traceId[{}] request [{}] session [{}]",
                    upTraceId,
                    requestId,
                    writeSession,
                    e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void remoteFlush(
            RemoteFlushRequest request, StreamObserver<RemoteFlushResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        long timeout = request.getWaitTimeMs();
        logger.info("flush snapshot id [{}] with timeout [{}]ms", snapshotId, timeout);
        try {
            boolean suc;
            if (snapshotId == 0L) {
                suc = graphWriter.flushLastSnapshot(timeout);
            } else {
                suc = graphWriter.flushSnapshot(snapshotId, timeout);
            }
            responseObserver.onNext(RemoteFlushResponse.newBuilder().setSuccess(suc).build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            logger.error(
                    "remoteFlush failed. flushSnapshotId [{}] waitTimeMs [{}]",
                    snapshotId,
                    timeout);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void replayRecords(
            ReplayRecordsRequest request, StreamObserver<ReplayRecordsResponse> responseObserver) {
        long offset = request.getOffset();
        long timestamp = request.getTimestamp();
        logger.info("replay records from offset {}, timestamp {}", offset, timestamp);
        try {
            List<Long> ids = graphWriter.replayWALFrom(offset, timestamp);
            responseObserver.onNext(
                    ReplayRecordsResponse.newBuilder().addAllSnapshotId(ids).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            logger.error("replayRecords failed", e);
            responseObserver.onError(e);
        }
    }
}
