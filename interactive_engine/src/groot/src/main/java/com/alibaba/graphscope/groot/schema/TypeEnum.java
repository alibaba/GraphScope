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
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.proto.groot.TypeEnumPb;

public enum TypeEnum {
    /**
     * vertex type
     */
    VERTEX(0),

    /**
     * edge type
     */
    EDGE(1);

    private final byte b;

    TypeEnum(int b) {
        this.b = (byte) b;
    }

    public static TypeEnum fromId(byte id) {
        if (id < 0 || id >= ALL.length) {
            throw new IllegalArgumentException("Unknown TypeEnum: [" + id + "]");
        }
        return ALL[id];
    }

    public static TypeEnum parseProto(TypeEnumPb proto) {
        return fromId((byte) proto.getNumber());
    }

    public TypeEnumPb toProto() {
        return TypeEnumPb.forNumber(b);
    }

    private static final TypeEnum[] ALL = TypeEnum.values();
}
