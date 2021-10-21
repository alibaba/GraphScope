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
package com.alibaba.graphscope.groot.operation;

import com.alibaba.maxgraph.proto.groot.EdgeIdPb;

public class EdgeId {

    private VertexId srcId;
    private VertexId dstId;
    private long id;

    public EdgeId(VertexId srcId, VertexId dstId, long id) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.id = id;
    }

    public VertexId getSrcId() {
        return srcId;
    }

    public VertexId getDstId() {
        return dstId;
    }

    public EdgeIdPb toProto() {
        return EdgeIdPb.newBuilder()
                .setSrcId(srcId.toProto())
                .setDstId(dstId.toProto())
                .setId(id)
                .build();
    }

    @Override
    public String toString() {
        return srcId + "-" + id + "->" + dstId;
    }
}
