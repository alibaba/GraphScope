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

public class FuzzyPatternVertex extends PatternVertex {

    private Integer id;
    private List<Integer> vertexTypeIds;

    public FuzzyPatternVertex(List<Integer> vertexTypeIds) {
        this(vertexTypeIds, 0);
    }

    public FuzzyPatternVertex(List<Integer> vertexTypeIds, int id) {
        this(vertexTypeIds, id, new ElementDetails());
    }

    public FuzzyPatternVertex(List<Integer> typeIds, int id, ElementDetails details) {
        super(details, new VertexIsomorphismChecker(typeIds, details));
        this.vertexTypeIds = typeIds;
        this.id = id;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public List<Integer> getVertexTypeIds() {
        return vertexTypeIds;
    }
}
