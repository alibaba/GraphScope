package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.proto.v2.PrepareDataLoadPb;
import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import com.google.protobuf.ByteString;

public class CommitDataLoadRequest extends AbstractDdlRequest {

    private DataLoadTarget dataLoadTarget;
    private long tableId;

    public CommitDataLoadRequest(DataLoadTarget dataLoadTarget, long tableId) {
        super(OperationType.COMMIT_DATA_LOAD);
        this.dataLoadTarget = dataLoadTarget;
        this.tableId = tableId;
    }

    @Override
    protected ByteString getBytes() {
        return PrepareDataLoadPb.newBuilder()
                .setTarget(dataLoadTarget.toProto())
                .setTableIdx(this.tableId)
                .build().toByteString();
    }
}
