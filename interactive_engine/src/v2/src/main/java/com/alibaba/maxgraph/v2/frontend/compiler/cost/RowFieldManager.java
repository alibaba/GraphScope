package com.alibaba.maxgraph.v2.frontend.compiler.cost;

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
