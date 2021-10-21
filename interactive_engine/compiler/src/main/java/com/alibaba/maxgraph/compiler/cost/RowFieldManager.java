/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.cost;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

public class RowFieldManager {
    private RowFieldManager parent;
    private RowFieldManager child = null;
    private RowField rowField;
    private Set<String> birthFieldList;

    public RowFieldManager(RowField rowField, RowFieldManager parent, Set<String> birthFieldList) {
        this.parent = parent;
        this.rowField = rowField;
        this.birthFieldList = Sets.newHashSet(birthFieldList);
        if (null != parent) {
            parent.setChild(this);
        }
    }

    public RowFieldManager(RowField rowField, Set<String> birthFieldList) {
        this(rowField, null, birthFieldList);
    }

    public RowField getRowField() {
        return rowField;
    }

    public void setChild(RowFieldManager child) {
        this.child = child;
    }

    public RowFieldManager getParent() {
        return parent;
    }

    public RowFieldManager getChild() {
        return this.child;
    }

    public Set<String> getBirthFieldList() {
        return ImmutableSet.copyOf(this.birthFieldList);
    }

    @Override
    public String toString() {
        return rowField.toString();
    }
}
