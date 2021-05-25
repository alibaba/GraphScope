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
package com.alibaba.maxgraph.v2.common.operation;

import com.alibaba.maxgraph.proto.v2.VertexIdPb;

public class VertexId {
    private long id;

    public VertexId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public VertexIdPb toProto() {
        return VertexIdPb.newBuilder().setId(id).build();
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
