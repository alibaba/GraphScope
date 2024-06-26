/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta;

import com.google.common.base.Objects;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class is used to uniquely identify a graph instance in scenarios that support multiple graphs.
 * @param <T>
 */
public class GraphId<T> {
    public static final GraphId DEFAULT = new GraphId(null);

    private final @Nullable T id;

    public GraphId(T id) {
        this.id = id;
    }

    public @Nullable T getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphId<?> graphId = (GraphId<?>) o;
        return Objects.equal(id, graphId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
