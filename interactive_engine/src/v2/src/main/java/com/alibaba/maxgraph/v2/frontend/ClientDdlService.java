package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.proto.v2.*;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import io.grpc.stub.StreamObserver;

public class ClientDdlService extends ClientDdlGrpc.ClientDdlImplBase {

    private SchemaWriter schemaWriter;
    private SnapshotCache snapshotCache;

    public ClientDdlService(SchemaWriter schemaWriter, SnapshotCache snapshotCache) {
        this.schemaWriter = schemaWriter;
        this.snapshotCache = snapshotCache;
    }

    @Override
    public void submitDdl(SubmitDdlRequest request, StreamObserver<SubmitDdlResponse> responseObserver) {
        DdlRequestBatchPb.Builder builder = DdlRequestBatchPb.newBuilder();
        for (DdlRequestPb ddlRequestPb : request.getDdlRequestsList()) {
            builder.addDdlRequests(ddlRequestPb);
        }
        long snapshotId = this.schemaWriter.submitBatchDdl(UuidUtils.getBase64UUIDString(), "", builder.build());
        this.snapshotCache.addListener(snapshotId, () -> {
            GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
            responseObserver.onNext(SubmitDdlResponse.newBuilder().setGraphDef(graphDef.toProto()).build());
            responseObserver.onCompleted();
        });
    }
}
