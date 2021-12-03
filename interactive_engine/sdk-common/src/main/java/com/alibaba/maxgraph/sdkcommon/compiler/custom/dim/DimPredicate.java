/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.sdkcommon.compiler.custom.dim;

import com.alibaba.maxgraph.sdkcommon.compiler.custom.CustomPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.PredicateType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class DimPredicate<S, E> extends CustomPredicate<S, E> {
    private static final long serialVersionUID = -6860361836719972123L;

    private DimTable dimTable;
    private DimMatchType dimMatchType;

    public DimPredicate() {
        this(null, null);
        setPredicateType(PredicateType.DIM);
    }

    public DimPredicate(DimTable dimTable, DimMatchType dimMatchType) {
        super(null, null);
        this.dimTable = dimTable;
        this.dimMatchType = dimMatchType;
        setPredicateType(PredicateType.DIM);
    }

    public DimTable getDimTable() {
        return dimTable;
    }

    public void setDimTable(DimTable dimTable) {
        this.dimTable = dimTable;
    }

    public DimMatchType getDimMatchType() {
        return dimMatchType;
    }

    public void setDimMatchType(DimMatchType dimMatchType) {
        this.dimMatchType = dimMatchType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DimPredicate<?, ?> that = (DimPredicate<?, ?>) o;
        return Objects.equal(dimTable, that.dimTable) &&
                dimMatchType == that.dimMatchType &&
                getPredicateType() == that.getPredicateType();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dimTable, dimMatchType, getPredicateType());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dimTable", dimTable)
                .add("dimMatchType", dimMatchType)
                .toString();
    }
}
