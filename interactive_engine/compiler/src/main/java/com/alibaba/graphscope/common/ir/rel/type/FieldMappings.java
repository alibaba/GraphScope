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

package com.alibaba.graphscope.common.ir.rel.type;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Define mappings between fields in the external source and fields in the optTable graph
 */
public class FieldMappings {
    protected final List<Entry> mappings;

    public FieldMappings(List<Entry> mappings) {
        this.mappings = Objects.requireNonNull(mappings);
    }

    public List<Entry> getMappings() {
        return Collections.unmodifiableList(this.mappings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMappings that = (FieldMappings) o;
        return com.google.common.base.Objects.equal(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(mappings);
    }

    @Override
    public String toString() {
        return "FieldMappings{" + "mappings=" + mappings + '}';
    }

    public static class Entry {
        private final RexNode source;
        private final RexNode target;

        public Entry(RexNode source, RexNode target) {
            this.source = Objects.requireNonNull(source);
            this.target = Objects.requireNonNull(target);
        }

        public RexNode getSource() {
            return source;
        }

        public RexNode getTarget() {
            return target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return com.google.common.base.Objects.equal(source, entry.source)
                    && com.google.common.base.Objects.equal(target, entry.target);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(source, target);
        }

        @Override
        public String toString() {
            return "Entry{" + "source=" + source + ", target=" + target + '}';
        }
    }
}
