/**
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/util/MutablePath.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.structure;

import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class MxPath implements Path {
    private final List<Object> objects;
    private final List<Set<String>> labels;

    public MxPath() {
        this(10);
    }

    private MxPath(final int capacity) {
        this.objects = new ArrayList<>(capacity);
        this.labels = new ArrayList<>(capacity);
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    public MxPath clone() {
        final MxPath clone = new MxPath(this.objects.size());
        // TODO: Why is this not working Hadoop serialization-wise?... Its cause DetachedPath's clone needs to detach on clone.
        /*final MutablePath clone = (MutablePath) super.clone();
        clone.objects = new ArrayList<>();
        clone.labels = new ArrayList<>();*/
        clone.objects.addAll(this.objects);
        for (final Set<String> labels : this.labels) {
            clone.labels.add(new LinkedHashSet<>(labels));
        }
        return clone;
    }


    @Override
    public int size() {
        return this.objects.size();
    }

    @Override
    public Path extend(final Object object, final Set<String> labels) {
        this.objects.add(object);
        this.labels.add(new LinkedHashSet<>(labels));
        return this;
    }

    @Override
    public Path extend(final Set<String> labels) {
        if (!labels.isEmpty())
            this.labels.get(this.labels.size() - 1).addAll(labels);
        return this;
    }

    @Override
    public Path retract(final Set<String> removeLabels) {
        for (int i = this.labels.size() - 1; i >= 0; i--) {
            this.labels.get(i).removeAll(removeLabels);
            if (this.labels.get(i).isEmpty()) {
                this.labels.remove(i);
                this.objects.remove(i);
            }
        }
        return this;
    }

    @Override
    public <A> A get(int index) {
        return (A) this.objects.get(index);
    }

    @Override
    public <A> A get(final Pop pop, final String label) {
        if (Pop.all == pop) {
            if (this.hasLabel(label)) {
                final Object object = this.get(label);
                if (object instanceof List)
                    return (A) object;
                else
                    return (A) Collections.singletonList(object);
            } else {
                return (A) Collections.emptyList();
            }
        } else {
            // Override default to avoid building temporary list, and to stop looking when we find the label.
            if (Pop.last == pop) {
                for (int i = this.labels.size() - 1; i >= 0; i--) {
                    if (labels.get(i).contains(label))
                        return (A) objects.get(i);
                }
            } else {
                for (int i = 0; i != this.labels.size(); i++) {
                    if (labels.get(i).contains(label))
                        return (A) objects.get(i);
                }
            }
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        }
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.labels.stream().filter(l -> l.contains(label)).findAny().isPresent();
    }

    @Override
    public List<Object> objects() {
        return Collections.unmodifiableList(this.objects);
    }

    @Override
    public List<Set<String>> labels() {
        return Collections.unmodifiableList(this.labels);
    }

    @Override
    public Iterator<Object> iterator() {
        return this.objects.iterator();
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MxPath objects1 = (MxPath) o;
        return Objects.equal(objects, objects1.objects) &&
                Objects.equal(labels, objects1.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(objects, labels);
    }
}
