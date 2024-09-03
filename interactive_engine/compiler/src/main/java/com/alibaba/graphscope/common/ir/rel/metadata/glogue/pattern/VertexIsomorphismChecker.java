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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class VertexIsomorphismChecker implements IsomorphismChecker<VertexIsomorphismChecker> {
    private final List<Integer> typeIds;
    private final ElementDetails details;

    public VertexIsomorphismChecker(List<Integer> typeIds, ElementDetails details) {
        Collections.sort(typeIds, Integer::compareTo);
        this.typeIds = typeIds;
        this.details = details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexIsomorphismChecker that = (VertexIsomorphismChecker) o;
        return Objects.equals(typeIds, that.typeIds) && Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeIds, details);
    }

    @Override
    public int compareTo(VertexIsomorphismChecker o) {
        if (this.equals(o)) {
            return 0;
        }
        if (this.typeIds.size() != o.typeIds.size()) {
            return this.typeIds.size() - o.typeIds.size();
        }
        for (int i = 0; i < this.typeIds.size(); i++) {
            if (!this.typeIds.get(i).equals(o.typeIds.get(i))) {
                return this.typeIds.get(i) - o.typeIds.get(i);
            }
        }
        return this.details.compareTo(o.details);
    }
}
