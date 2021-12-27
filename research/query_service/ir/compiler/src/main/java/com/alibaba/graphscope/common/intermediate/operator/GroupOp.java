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

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class GroupOp extends InterOpBase {
    // List of Pair<FfiVariable, FfiAlias>
    private Optional<OpArg> groupByKeys;

    // List<ArgAggFn>
    private Optional<OpArg> groupByValues;

    public GroupOp() {
        super();
        groupByKeys = Optional.empty();
        groupByValues = Optional.empty();
    }

    public Optional<OpArg> getGroupByKeys() {
        return groupByKeys;
    }

    public Optional<OpArg> getGroupByValues() {
        return groupByValues;
    }

    public void setGroupByKeys(OpArg groupByKeys) {
        this.groupByKeys = Optional.of(groupByKeys);
    }

    public void setGroupByValues(OpArg groupByValues) {
        this.groupByValues = Optional.of(groupByValues);
    }
}
