/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema.request;

import com.alibaba.maxgraph.proto.groot.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.groot.DdlRequestPb;
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
