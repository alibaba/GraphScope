/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema.request;

import com.alibaba.maxgraph.proto.groot.DdlRequestPb;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.google.common.base.Objects;
import com.google.protobuf.ByteString;

public class DdlRequestBlob {

    private OperationType operationType;
    private ByteString bytes;

    public DdlRequestBlob(OperationType operationType, ByteString bytes) {
        this.operationType = operationType;
        this.bytes = bytes;
    }

    public static DdlRequestBlob parseProto(DdlRequestPb proto) {
        OperationType operationType = OperationType.parseProto(proto.getOpType());
        return new DdlRequestBlob(operationType, proto.getDdlBytes());
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public ByteString getBytes() {
        return bytes;
    }

    public DdlRequestPb toProto() {
        return DdlRequestPb.newBuilder()
                .setOpType(operationType.toProto())
                .setDdlBytes(bytes)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DdlRequestBlob that = (DdlRequestBlob) o;
        return operationType == that.operationType && Objects.equal(bytes, that.bytes);
    }
}
