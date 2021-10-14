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

import com.alibaba.maxgraph.proto.groot.OpTypePb;

public enum OperationType {
    MARKER(0),

    OVERWRITE_VERTEX(1),
    UPDATE_VERTEX(2),
    DELETE_VERTEX(3),
    OVERWRITE_EDGE(4),
    UPDATE_EDGE(5),
    DELETE_EDGE(6),

    CREATE_VERTEX_TYPE(7),
    CREATE_EDGE_TYPE(8),
    ADD_EDGE_KIND(9),

    DROP_VERTEX_TYPE(10),
    DROP_EDGE_TYPE(11),
    REMOVE_EDGE_KIND(12),

    PREPARE_DATA_LOAD(13),
    COMMIT_DATA_LOAD(14);

    private final byte b;

    OperationType(int b) {
        this.b = (byte) b;
    }

    public byte getId() {
        return b;
    }

    public static OperationType fromId(byte id) {
        if (id < 0 || id >= TYPES.length) {
            throw new IllegalArgumentException("Unknown OperationType: [" + id + "]");
        }
        return TYPES[id];
    }

    public static OperationType parseProto(OpTypePb pb) {
        return fromId((byte) pb.getNumber());
    }

    public OpTypePb toProto() {
        return OpTypePb.forNumber(b);
    }

    private static final OperationType[] TYPES = OperationType.values();
}
