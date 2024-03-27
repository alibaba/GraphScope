/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

import java.util.List;

public abstract class PatternEdge {
    private final boolean isBoth;
    private final ElementDetails elementDetails;
    private final IsomorphismChecker isomorphismChecker;

    protected PatternEdge(
            boolean isBoth, ElementDetails elementDetails, IsomorphismChecker isomorphismChecker) {
        this.isBoth = isBoth;
        this.elementDetails = elementDetails;
        this.isomorphismChecker = isomorphismChecker;
    }

    public abstract PatternVertex getSrcVertex();

    public abstract PatternVertex getDstVertex();

    public abstract Integer getId();

    public abstract List<EdgeTypeId> getEdgeTypeIds();

    public boolean isBoth() {
        return this.isBoth;
    }

    public ElementDetails getElementDetails() {
        return elementDetails;
    }

    public IsomorphismChecker getIsomorphismChecker() {
        return isomorphismChecker;
    }

    @Override
    public String toString() {
        return getSrcVertex().getId()
                + "->"
                + getDstVertex().getId()
                + "["
                + getEdgeTypeIds().toString()
                + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternEdge)) {
            return false;
        }
        PatternEdge other = (PatternEdge) o;
        return this.getSrcVertex().equals(other.getSrcVertex())
                && this.getDstVertex().equals(other.getDstVertex())
                && this.getEdgeTypeIds().equals(other.getEdgeTypeIds());
    }
}
