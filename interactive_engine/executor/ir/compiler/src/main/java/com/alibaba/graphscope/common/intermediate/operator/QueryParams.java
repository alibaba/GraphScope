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

import com.alibaba.graphscope.common.jna.type.FfiNameOrId;

import org.javatuples.Pair;

import java.util.*;

// Source/Expand/GetV need params to query from store, compatible with graph interface
public class QueryParams {
    public static String SNAPSHOT_CONFIG_NAME = "SID";

    private List<FfiNameOrId.ByValue> tables;

    private List<FfiNameOrId.ByValue> columns;

    private boolean isAllColumns;

    // lower and upper
    private Optional<Pair<Integer, Integer>> range;

    private Optional<String> predicate;

    private Map<String, String> extraParams;

    public QueryParams() {
        this.predicate = Optional.empty();
        this.range = Optional.empty();
        this.tables = new ArrayList<>();
        this.columns = new ArrayList<>();
        this.extraParams = new HashMap<>();
        this.isAllColumns = false;
    }

    public void addTable(FfiNameOrId.ByValue name) {
        this.tables.add(name);
    }

    public void addColumn(FfiNameOrId.ByValue name) {
        this.columns.add(name);
    }

    public void setPredicate(String predicate) {
        if (predicate != null) {
            this.predicate = Optional.of(predicate);
        }
    }

    public void setRange(Pair<Integer, Integer> range) {
        if (range != null) {
            this.range = Optional.of(range);
        }
    }

    public void setAllColumns(boolean allColumns) {
        isAllColumns = allColumns;
    }

    public List<FfiNameOrId.ByValue> getTables() {
        return Collections.unmodifiableList(tables);
    }

    public List<FfiNameOrId.ByValue> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public Optional<Pair<Integer, Integer>> getRange() {
        return range;
    }

    public Optional<String> getPredicate() {
        return predicate;
    }

    public void addExtraParams(String key, String value) {
        this.extraParams.put(key, value);
    }

    public Map<String, String> getExtraParams() {
        return Collections.unmodifiableMap(extraParams);
    }

    public boolean isAllColumns() {
        return isAllColumns;
    }
}
