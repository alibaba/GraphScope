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

import java.util.Objects;

public class ElementDetails implements Comparable<ElementDetails> {
    private final double selectivity;

    public ElementDetails() {
        this(1.0d);
    }

    public ElementDetails(double selectivity) {
        this.selectivity = selectivity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElementDetails that = (ElementDetails) o;
        return Double.compare(that.selectivity, selectivity) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectivity);
    }

    public double getSelectivity() {
        return selectivity;
    }

    @Override
    public int compareTo(ElementDetails o) {
        return Double.compare(selectivity, o.selectivity);
    }
}
