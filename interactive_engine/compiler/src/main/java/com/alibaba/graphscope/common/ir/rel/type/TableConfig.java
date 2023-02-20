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

package com.alibaba.graphscope.common.ir.rel.type;

import org.apache.calcite.plan.RelOptTable;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * For each vertex or edge, we derive all the table(s) it requests depending on its query given label(s)
 */
public class TableConfig {
    // indicate whether all labels are requested, i.e. g.V()
    private boolean isAll;
    // we have derived the table type(s) for the query given label(s), should not be empty
    private List<RelOptTable> tables;

    public TableConfig(List<RelOptTable> tables) {
        this.tables = ObjectUtils.requireNonEmpty(tables);
        ;
        this.isAll = false;
    }

    public TableConfig isAll(boolean isAll) {
        this.isAll = isAll;
        return this;
    }

    public boolean isAll() {
        return isAll;
    }

    public List<RelOptTable> getTables() {
        return Collections.unmodifiableList(this.tables);
    }

    @Override
    public String toString() {
        List<String> labelNames =
                tables.stream()
                        .filter(k -> ObjectUtils.isNotEmpty(k.getQualifiedName()))
                        .map(k -> k.getQualifiedName().get(0))
                        .collect(Collectors.toList());
        return "{" + "isAll=" + isAll + ", tables=" + labelNames + '}';
    }
}
