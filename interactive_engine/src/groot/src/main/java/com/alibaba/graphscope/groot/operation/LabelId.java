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

import com.alibaba.maxgraph.proto.groot.LabelIdPb;

public class LabelId {
    private int id;

    public LabelId(int id) {
        this.id = id;
    }

    public static LabelId parseProto(LabelIdPb proto) {
        int id = proto.getId();
        return new LabelId(id);
    }

    public int getId() {
        return id;
    }

    public LabelIdPb toProto() {
        return LabelIdPb.newBuilder().setId(id).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LabelId labelId = (LabelId) o;

        return id == labelId.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "LabelId{" + "id=" + id + '}';
    }
}
