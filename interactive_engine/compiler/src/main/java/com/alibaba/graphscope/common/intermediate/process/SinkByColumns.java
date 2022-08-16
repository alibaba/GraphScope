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

package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.jna.type.FfiNameOrId;

import java.util.*;

public class SinkByColumns implements SinkArg {
    private List<FfiNameOrId.ByValue> columnNames;

    public SinkByColumns() {
        columnNames = new ArrayList<>();
    }

    public List<FfiNameOrId.ByValue> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    public void addColumnName(FfiNameOrId.ByValue columnName) {
        this.columnNames.add(columnName);
    }

    public void dedup() {
        Set<FfiNameOrId.ByValue> uniqueColumnNames = new HashSet<>(columnNames);
        columnNames.clear();
        columnNames.addAll(uniqueColumnNames);
    }
}
