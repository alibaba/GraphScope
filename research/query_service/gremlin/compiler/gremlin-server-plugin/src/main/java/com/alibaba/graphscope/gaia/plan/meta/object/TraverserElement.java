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
package com.alibaba.graphscope.gaia.plan.meta.object;

import com.google.common.base.Objects;

import java.util.UUID;

public class TraverserElement {
    private UUID uuid;
    // composite class
    private CompositeObject object;

    public TraverserElement(CompositeObject object) {
        this.object = object;
        this.uuid = UUID.randomUUID();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraverserElement that = (TraverserElement) o;
        return Objects.equal(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uuid);
    }

    public CompositeObject getObject() {
        return object;
    }

    public TraverserElement fork() {
        if (!object.isGraphElement()) {
            return new TraverserElement(object);
        } else if (object.getElement() instanceof Vertex) {
            return new TraverserElement(new CompositeObject(new Vertex()));
        } else if (object.getElement() instanceof Edge) {
            return new TraverserElement(new CompositeObject(new Edge()));
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
