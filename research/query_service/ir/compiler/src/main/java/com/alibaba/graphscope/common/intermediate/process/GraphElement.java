/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.intermediate.process;

import com.google.common.base.Objects;

import java.util.UUID;

public class GraphElement {
    // edge or vertex
    private boolean isEdge;
    // unique id, for hasCode in map
    private UUID uniqueId;

    public GraphElement(boolean isEdge) {
        this.isEdge = isEdge;
        this.uniqueId = UUID.randomUUID();
    }

    public boolean isEdge() {
        return isEdge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return isEdge == that.isEdge &&
                Objects.equal(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(isEdge, uniqueId);
    }
}
