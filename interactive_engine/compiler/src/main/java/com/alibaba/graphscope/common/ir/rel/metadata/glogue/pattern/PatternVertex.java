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

import java.util.List;

public abstract class PatternVertex {
    private final ElementDetails details;
    private final IsomorphismChecker isomorphismChecker;

    protected PatternVertex(ElementDetails details, IsomorphismChecker isomorphismChecker) {
        this.details = details;
        this.isomorphismChecker = isomorphismChecker;
    }

    public abstract Integer getId();

    public abstract List<Integer> getVertexTypeIds();

    public IsomorphismChecker getIsomorphismChecker() {
        return isomorphismChecker;
    }

    public ElementDetails getElementDetails() {
        return details;
    }

    @Override
    public String toString() {
        return getId().toString() + "[" + getVertexTypeIds().toString() + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PatternVertex)) {
            return false;
        }
        PatternVertex other = (PatternVertex) o;
        return this.getId().equals(other.getId())
                && this.getVertexTypeIds().equals(other.getVertexTypeIds());
    }
}
