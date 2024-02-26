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

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class SinglePatternVertex extends PatternVertex {
    private Integer vertexTypeId;
    private Integer id;

    public SinglePatternVertex(Integer vertexTypeId) {
        this(vertexTypeId, 0);
    }

    public SinglePatternVertex(Integer vertexTypeId, int id) {
        this(vertexTypeId, id, new ElementDetails());
    }

    public SinglePatternVertex(Integer typeId, int id, ElementDetails details) {
        super(details, new VertexIsomorphismChecker(Lists.newArrayList(typeId), details));
        this.vertexTypeId = typeId;
        this.id = id;
    }

    @Override
    public List<Integer> getVertexTypeIds() {
        return Arrays.asList(vertexTypeId);
    }

    @Override
    public Integer getId() {
        return id;
    }
}
