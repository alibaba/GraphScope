package com.alibaba.maxgraph.v2.frontend.compiler.cost;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

public class RowField {
    private static RowField EMPTY_ROW_FIELD = new RowField(Sets.newHashSet());

    private Set<String> fieldList = Sets.newHashSet();

    public RowField(String field) {
        this.fieldList.add(field);
    }

    public RowField(Set<String> fieldList) {
        this.fieldList.addAll(fieldList);
    }

    public Set<String> getFieldList() {
        return this.fieldList;
    }

    public static RowField emptyRowField() {
        return EMPTY_ROW_FIELD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowField rowField = (RowField) o;
        return Objects.equal(fieldList, rowField.fieldList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldList);
    }

    @Override
    public String toString() {
        return StringUtils.join(fieldList, ";");
    }
}
