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
package com.alibaba.maxgraph.v2.common.frontend.result;

import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.google.common.base.Joiner;

/**
 * Implemetion of element id for vertex/edge in maxgraph
 */
public class CompositeId implements ElementId {
    private long id;
    private int labelId;

    public CompositeId() {

    }

    public CompositeId(long id, int labelId) {
        this.id = id;
        this.labelId = labelId;
    }


    @Override
    public long id() {
        return this.id;
    }

    @Override
    public int labelId() {
        return this.labelId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeId remoteId = (CompositeId) o;

        if (id != remoteId.id()) {
            return false;
        }
        return labelId == remoteId.labelId();
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + labelId;
        return result;
    }

    @Override
    public String toString() {
        return Joiner.on(".").join(labelId, id);
    }
}