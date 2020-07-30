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
package com.alibaba.maxgraph.structure;


import com.alibaba.maxgraph.sdkcommon.graph.ElementId;

public class LongId implements ElementId {
    private long id;

    public LongId(long id) {
        this.id = id;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public int typeId() {
        return (int)(id & 0xFFFF);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        LongId longId = (LongId)o;

        return id == longId.id;
    }

    @Override
    public int hashCode() {
        return (int)(id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return id + "";
    }
}
