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

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
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
    private final GraphOpt.PathExpandResult resultOpt;
    private final GraphOpt.PathExpandPath pathOpt;
    private boolean optional;

    public ElementDetails() {
        this(1.0d);
    }

    public ElementDetails(double selectivity) {
        this(selectivity, null, ImmutableList.of(), null, null);
    }

    public ElementDetails(
            double selectivity,
            @Nullable PathExpandRange range,
            List<Integer> pxdInnerGetVTypes,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt) {
        this(selectivity, range, pxdInnerGetVTypes, resultOpt, pathOpt, false);
    }

    public ElementDetails(double selectivity, boolean optional) {
        this(selectivity, null, ImmutableList.of(), null, null, optional);
    }

    public ElementDetails(
            double selectivity,
            @Nullable PathExpandRange range,
            List<Integer> pxdInnerVertexTypes,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            boolean optional) {
        this.selectivity = selectivity;
        this.range = range;
        this.pxdInnerGetVTypes = pxdInnerVertexTypes;
        this.optional = optional;
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElementDetails details = (ElementDetails) o;
        return Double.compare(details.selectivity, selectivity) == 0
                && Objects.equals(range, details.range)
                && Objects.equals(pxdInnerGetVTypes, details.pxdInnerGetVTypes)
                && optional == details.optional
                && Objects.equals(resultOpt, details.resultOpt)
                && Objects.equals(pathOpt, details.pathOpt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectivity, range, optional, pxdInnerGetVTypes, resultOpt, pathOpt);
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

    public GraphOpt.PathExpandResult getResultOpt() {
        return resultOpt;
    }

    public GraphOpt.PathExpandPath getPathOpt() {
        return pathOpt;
    }

    @Override
    public int compareTo(ElementDetails o) {
        return o == null ? -1 : this.hashCode() - o.hashCode();
    }
}
