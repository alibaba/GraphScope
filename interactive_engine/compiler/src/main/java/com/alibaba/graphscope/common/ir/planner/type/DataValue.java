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

package com.alibaba.graphscope.common.ir.planner.type;

import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DataValue {
    private final String alias;
    private final @Nullable RexNode filter;
    // indicate whether the value corresponds to a split path expand, the parentAlias here records
    // the alias
    // associated with the original path expand before the splitting.
    private final @Nullable String parentAlias;

    public DataValue(String alias, RexNode filter) {
        this(alias, filter, null);
    }

    public DataValue(String alias, RexNode filter, String parentAlias) {
        this.alias = alias;
        this.filter = filter;
        this.parentAlias = parentAlias;
    }

    public String getAlias() {
        return alias;
    }

    public @Nullable RexNode getFilter() {
        return filter;
    }

    public @Nullable String getParentAlias() {
        return parentAlias;
    }

    @Override
    public String toString() {
        return "DataValue{" + "alias='" + alias + '\'' + ", filter=" + filter + '}';
    }
}
