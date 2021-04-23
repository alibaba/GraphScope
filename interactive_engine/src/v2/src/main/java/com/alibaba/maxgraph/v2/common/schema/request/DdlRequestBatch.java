package com.alibaba.maxgraph.v2.common.schema.request;

import com.alibaba.maxgraph.proto.v2.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.v2.DdlRequestPb;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.OperationBlob;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DdlRequestBatch implements Iterable<DdlRequestBlob> {
    private List<DdlRequestBlob> ddlRequestBlobs;

    private DdlRequestBatch(List<DdlRequestBlob> ddlRequestBlobs) {
        this.ddlRequestBlobs = ddlRequestBlobs;
    }

    public static DdlRequestBatch parseProto(DdlRequestBatchPb proto) {
        List<DdlRequestPb> ddlRequestPbs = proto.getDdlRequestsList();
        List<DdlRequestBlob> ddlRequestBlobs = new ArrayList<>(ddlRequestPbs.size());
        for (DdlRequestPb ddlRequestPb : ddlRequestPbs) {
            ddlRequestBlobs.add(DdlRequestBlob.parseProto(ddlRequestPb));
        }
        return new DdlRequestBatch(ddlRequestBlobs);
    }

    public DdlRequestBatchPb toProto() {
        DdlRequestBatchPb.Builder builder = DdlRequestBatchPb.newBuilder();
        for (DdlRequestBlob blob : ddlRequestBlobs) {
            builder.addDdlRequests(blob.toProto());
        }
        return builder.build();
    }

    @Override
    public Iterator<DdlRequestBlob> iterator() {
        return ddlRequestBlobs.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        DdlRequestBatch that = (DdlRequestBatch) o;
        return Objects.equal(ddlRequestBlobs, that.ddlRequestBlobs);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private List<DdlRequestBlob> ddlRequestBlobs;

        private Builder() {
            this.ddlRequestBlobs = new ArrayList<>();
        }

        public Builder addDdlRequest(AbstractDdlRequest ddlRequest) {
            this.ddlRequestBlobs.add(ddlRequest.toBlob());
            return this;
        }

        public DdlRequestBatch build() {
            return new DdlRequestBatch(new ArrayList<>(ddlRequestBlobs));
        }
    }

}
