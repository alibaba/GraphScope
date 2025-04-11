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

package com.alibaba.graphscope.common.ir.runtime.write;

import com.alibaba.graphscope.interactive.models.EdgeRequest;

import org.jetbrains.annotations.NotNull;

import java.util.*;

public class BatchEdgeRequest implements List<EdgeRequest> {
    private final List<EdgeRequest> inner;

    public BatchEdgeRequest() {
        this.inner = new ArrayList<>();
    }

    @Override
    public int size() {
        return this.inner.size();
    }

    @Override
    public boolean isEmpty() {
        return this.inner.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.inner.contains(o);
    }

    @NotNull
    @Override
    public Iterator<EdgeRequest> iterator() {
        return this.inner.iterator();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return this.inner.toArray();
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull T[] a) {
        return this.inner.toArray(a);
    }

    @Override
    public boolean add(EdgeRequest edgeRequest) {
        return this.inner.add(edgeRequest);
    }

    @Override
    public boolean remove(Object o) {
        return this.inner.remove(o);
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        return this.inner.containsAll(c);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends EdgeRequest> c) {
        return this.inner.addAll(c);
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends EdgeRequest> c) {
        return this.inner.addAll(index, c);
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        return this.inner.removeAll(c);
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        return this.inner.retainAll(c);
    }

    @Override
    public void clear() {
        this.inner.clear();
    }

    @Override
    public EdgeRequest get(int index) {
        return this.inner.get(index);
    }

    @Override
    public EdgeRequest set(int index, EdgeRequest element) {
        return this.inner.set(index, element);
    }

    @Override
    public void add(int index, EdgeRequest element) {
        this.inner.add(index, element);
    }

    @Override
    public EdgeRequest remove(int index) {
        return this.inner.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.inner.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.inner.lastIndexOf(o);
    }

    @NotNull
    @Override
    public ListIterator<EdgeRequest> listIterator() {
        return this.inner.listIterator();
    }

    @NotNull
    @Override
    public ListIterator<EdgeRequest> listIterator(int index) {
        return this.inner.listIterator(index);
    }

    @NotNull
    @Override
    public List<EdgeRequest> subList(int fromIndex, int toIndex) {
        return this.inner.subList(fromIndex, toIndex);
    }
}
