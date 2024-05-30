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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ElementDetails implements Comparable<ElementDetails> {
    private final double selectivity;
    // the range is not null if and only if the element denotes a path expand operator
    private final @Nullable PathExpandRange range;
    // record inner getV types of path expand
    private final List<Integer> pxdInnerGetVTypes;
    private boolean optional;

    public ElementDetails() {
        this(1.0d);
    }

    public ElementDetails(double selectivity) {
        this(selectivity, null, ImmutableList.of());
    }

    public ElementDetails(
            double selectivity, @Nullable PathExpandRange range, List<Integer> pxdInnerGetVTypes) {
        this(selectivity, range, pxdInnerGetVTypes, false);
    }

    public ElementDetails(double selectivity, boolean optional) {
        this(selectivity, null, ImmutableList.of(), optional);
    }

    public ElementDetails(
            double selectivity,
            @Nullable PathExpandRange range,
            List<Integer> pxdInnerVertexTypes,
            boolean optional) {
        this.selectivity = selectivity;
        this.range = range;
        this.pxdInnerGetVTypes = pxdInnerVertexTypes;
        this.optional = optional;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElementDetails details = (ElementDetails) o;
        return Double.compare(details.selectivity, selectivity) == 0
                && Objects.equals(range, details.range)
                && Objects.equals(pxdInnerGetVTypes, details.pxdInnerGetVTypes)
                && optional == details.optional;
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectivity, range, optional);
    }

    public double getSelectivity() {
        return selectivity;
    }

    public @Nullable PathExpandRange getRange() {
        return range;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public List<Integer> getPxdInnerGetVTypes() {
        return Collections.unmodifiableList(this.pxdInnerGetVTypes);
    }

    @Override
    public int compareTo(ElementDetails o) {
        int compare = Double.compare(this.selectivity, o.selectivity);
        if (compare != 0) {
            return compare;
        }
        compare = Boolean.compare(this.optional, o.optional);
        if (compare != 0) {
            return compare;
        }
        if (!this.pxdInnerGetVTypes.equals(o.pxdInnerGetVTypes)) {
            return -1;
        }
        if (this.range != null && o.range != null) {
            return this.range.compareTo(o.range);
        }
        return this.range == null && o.range == null ? 0 : this.range == null ? -1 : 1;
    }
}
