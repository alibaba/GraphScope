package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.v2.common.operation.OperationType;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import com.google.protobuf.ByteString;

public class PrepareDataLoadRequest extends AbstractDdlRequest {

    private DataLoadTarget dataLoadTarget;

    public PrepareDataLoadRequest(DataLoadTarget dataLoadTarget) {
        super(OperationType.PREPARE_DATA_LOAD);
        this.dataLoadTarget = dataLoadTarget;
    }

    @Override
    protected ByteString getBytes() {
        return dataLoadTarget.toProto().toByteString();
    }
}
